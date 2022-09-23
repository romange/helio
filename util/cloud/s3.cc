// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/s3.h"

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include <absl/strings/match.h>

#include "base/logging.h"
#include "util/cloud/aws.h"
#include "util/proactor_base.h"

namespace util {
namespace cloud {

using namespace std;
namespace h2 = boost::beast::http;
using nonstd::make_unexpected;

const char kRootDomain[] = "s3.amazonaws.com";

inline std::string_view std_sv(const ::boost::beast::string_view s) {
  return std::string_view{s.data(), s.size()};
}

namespace xml {

inline xmlDocPtr XmlRead(string_view xml) {
  return xmlReadMemory(xml.data(), xml.size(), NULL, NULL, XML_PARSE_COMPACT | XML_PARSE_NOBLANKS);
}

inline const char* as_char(const xmlChar* var) {
  return reinterpret_cast<const char*>(var);
}

vector<string> ParseXmlListBuckets(string_view xml_resp) {
  xmlDocPtr doc = XmlRead(xml_resp);
  CHECK(doc);

  xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);

  auto register_res = xmlXPathRegisterNs(xpathCtx, BAD_CAST "NS",
                                         BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
  CHECK_EQ(register_res, 0);

  xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(
      BAD_CAST "/NS:ListAllMyBucketsResult/NS:Buckets/NS:Bucket/NS:Name", xpathCtx);
  CHECK(xpathObj);
  xmlNodeSetPtr nodes = xpathObj->nodesetval;
  vector<string> res;
  if (nodes) {
    int size = nodes->nodeNr;
    for (int i = 0; i < size; ++i) {
      xmlNodePtr cur = nodes->nodeTab[i];
      CHECK_EQ(XML_ELEMENT_NODE, cur->type);
      CHECK(cur->ns);
      CHECK(nullptr == cur->content);

      if (cur->children && cur->last == cur->children && cur->children->type == XML_TEXT_NODE) {
        CHECK(cur->children->content);
        res.push_back(as_char(cur->children->content));
      }
    }
  }

  xmlXPathFreeObject(xpathObj);
  xmlXPathFreeContext(xpathCtx);
  xmlFreeDoc(doc);

  return res;
}

std::pair<size_t, string_view> ParseXmlObjContents(xmlNodePtr node) {
  std::pair<size_t, string_view> res;

  for (xmlNodePtr child = node->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;

      if (!strcmp(as_char(child->name), "Key")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        res.second = string_view(as_char(grand->content));
      } else if (!strcmp(as_char(child->name), "Size")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        CHECK(absl::SimpleAtoi(as_char(grand->content), &res.first));
      }
    }
  }
  return res;
}

error_code ParseListObj(string_view xml_obj, S3Bucket::ListObjectCb cb) {
  xmlDocPtr doc = XmlRead(xml_obj);

  if (!doc) {
    LOG(ERROR) << "Could not parse xml response " << xml_obj;
    return make_error_code(errc::bad_message);
  }

  xmlNodePtr root = xmlDocGetRootElement(doc);
  CHECK_STREQ("ListBucketResult", as_char(root->name));

  for (xmlNodePtr child = root->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;
      if (!strcmp(as_char(child->name), "IsTruncated")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        CHECK_STREQ("false", as_char(grand->content)) << "TBD: need to support cursor based functionality";
      } else if (!strcmp(as_char(child->name), "Marker")) {
      } else if (!strcmp(as_char(child->name), "Contents")) {
        auto sz_name = ParseXmlObjContents(child);
        cb(sz_name.first, sz_name.second);
      }
    }
  }
  xmlFreeDoc(doc);

  return error_code{};
}

}  // namespace xml

ListBucketsResult ListS3Buckets(const AWS& aws, http::Client* http_client) {
  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  req.set(h2::field::host, kRootDomain);
  h2::response<h2::string_body> resp;

  aws.Sign(AWS::kEmptySig, &req);

  VLOG(1) << "Req: " << req;

  error_code ec = http_client->Send(req, &resp);
  if (ec)
    return make_unexpected(ec);

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "http error: " << resp;
    return make_unexpected(make_error_code(errc::inappropriate_io_control_operation));
  }

  VLOG(1) << "ListS3Buckets: " << resp;

  return xml::ParseXmlListBuckets(resp.body());
}

S3Bucket::S3Bucket(const AWS& aws, std::string_view bucket) : aws_(aws), bucket_(bucket) {
}

std::error_code S3Bucket::Connect(uint32_t ms) {
  ProactorBase* pb = ProactorBase::me();
  CHECK(pb);

  string host = GetHost();
  http_client_.reset(new http::Client{pb});
  http_client_->set_connect_timeout_ms(ms);

  VLOG(1) << "Connecting to " << host;

  return http_client_->Connect(host, "80");
}

error_code S3Bucket::ListObjects(string_view path, ListObjectCb cb) {
  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  req.set(h2::field::host, http_client_->host());

  h2::response<h2::string_body> resp;

  aws_.Sign(AWS::kEmptySig, &req);

  VLOG(1) << "Req: " << req;

  error_code ec = http_client_->Send(req, &resp);
  if (ec)
    return ec;

  if (resp.result() == h2::status::moved_permanently) {
    boost::beast::string_view new_region = resp["x-amz-bucket-region"];
    if (new_region.empty()) {
      return make_error_code(errc::network_unreachable);
    }
    aws_.UpdateRegion(std_sv(new_region));
    string host = GetHost();

    VLOG(1) << "Redirecting to " << host;
    ec = http_client_->Connect(host, "80");
    if (ec)
      return ec;

    req.set(h2::field::host, host);
    aws_.Sign(AWS::kEmptySig, &req);

    ec = http_client_->Send(req, &resp);
    if (ec)
      return ec;
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "http error: " << resp;
    return make_error_code(errc::inappropriate_io_control_operation);
  }

  if (!absl::StartsWith(resp.body(), "<?xml"))
    return make_error_code(errc::bad_message);

  // Sometimes s3 response contains multiple xml documents. xml2lib does not know how to parse them
  // or I did not find the way to do it. So I just check for another marker.
  string_view xml(resp.body());
  auto pos = xml.find("<?xml", 6);
  if (pos != string_view::npos) {
    VLOG(1) << "Removing preliminary xml " << xml.substr(0, pos);
    xml.remove_prefix(pos);
  }
  VLOG(1) << "ObjListResp: " << xml;
  return xml::ParseListObj(xml, std::move(cb));
}

std::string S3Bucket::GetHost() const {
  return absl::StrCat(bucket_, ".s3.", aws_.region(), ".amazonaws.com");
}

}  // namespace cloud
}  // namespace util
