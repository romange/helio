// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/s3.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "base/logging.h"
#include "util/cloud/aws.h"
#include "util/cloud/cloud_utils.h"
#include "util/cloud/s3_file.h"
#include "util/http/encoding.h"
#include "util/proactor_base.h"

namespace util {
namespace cloud {

using namespace std;
namespace h2 = boost::beast::http;
using nonstd::make_unexpected;

// Max number of keys in AWS response.
const unsigned kAwsMaxKeys = 1000;

inline std::string_view std_sv(const ::boost::beast::string_view s) {
  return std::string_view{s.data(), s.size()};
}

bool IsAwsEndpoint(string_view endpoint) {
  return absl::EndsWith(endpoint, ".amazonaws.com");
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

ListObjectsResult ParseListObj(string_view xml_obj, S3Bucket::ListObjectCb cb) {
  xmlDocPtr doc = XmlRead(xml_obj);

  bool truncated = false;
  string_view last_key = "";

  if (!doc) {
    LOG(ERROR) << "Could not parse xml response " << xml_obj;
    return make_unexpected(make_error_code(errc::bad_message));
  }

  absl::Cleanup xml_free{[doc]() { xmlFreeDoc(doc); }};

  xmlNodePtr root = xmlDocGetRootElement(doc);
  CHECK_STREQ("ListBucketResult", as_char(root->name));
  for (xmlNodePtr child = root->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;
      if (as_char(child->name) == "IsTruncated"sv) {
        truncated = as_char(grand->content) == "true"sv;
      } else if (as_char(child->name) == "Marker"sv) {
      } else if (as_char(child->name) == "Contents"sv) {
        auto sz_name = ParseXmlObjContents(child);
        cb(sz_name.first, sz_name.second);
        last_key = sz_name.second;
      }
    }
  }

  return truncated ? std::string{last_key} : "";
}

}  // namespace xml

ListBucketsResult ListS3Buckets(AWS* aws, http::Client* http_client) {
  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  req.set(h2::field::host, http_client->host());
  h2::response<h2::string_body> resp;

  auto ec = SendRequest(aws, http_client, &req, &resp);
  if (ec) {
    return make_unexpected(ec);
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "http error: " << resp;
    return make_unexpected(make_error_code(errc::inappropriate_io_control_operation));
  }

  VLOG(1) << "ListS3Buckets: " << resp;

  return xml::ParseXmlListBuckets(resp.body());
}

S3Bucket::S3Bucket(const AWS& aws, string_view bucket) : aws_(aws), bucket_(bucket) {
}

S3Bucket::S3Bucket(const AWS& aws, string_view endpoint, string_view bucket)
    : S3Bucket(aws, bucket) {
  endpoint_ = endpoint;
}

std::error_code S3Bucket::Connect(uint32_t ms) {
  ProactorBase* pb = ProactorBase::me();
  CHECK(pb);

  http_client_.reset(new http::Client{pb});
  http_client_->set_connect_timeout_ms(ms);

  return ConnectInternal();
}

ListObjectsResult S3Bucket::ListObjects(string_view bucket_path, ListObjectCb cb,
                                        std::string_view marker, unsigned max_keys) {
  CHECK(max_keys <= kAwsMaxKeys);

  string host = http_client_->host();
  std::string path;

  // Build full request path.
  if (IsAwsEndpoint(host)) {
    path.append("/?");
  } else {
    path.append("/").append(bucket_).append("?");
  }

  if (bucket_path != "")
    path += absl::StrCat("prefix=", util::http::UrlEncode(bucket_path), "&");

  if (marker != "")
    path += absl::StrCat("marker=", util::http::UrlEncode(marker), "&");

  if (max_keys != kAwsMaxKeys)
    path += absl::StrCat("max-keys=", max_keys, "&");

  CHECK(path.back() == '?' || path.back() == '&');
  path.pop_back();

  // Send request.
  h2::request<h2::empty_body> req{h2::verb::get, path, 11};
  req.set(h2::field::host, host);

  h2::response<h2::string_body> resp;

  error_code ec = SendRequest(&aws_, http_client_.get(), &req, &resp);
  if (ec)
    return make_unexpected(ec);

  if (resp.result() == h2::status::moved_permanently) {
    boost::beast::string_view new_region = resp["x-amz-bucket-region"];
    if (new_region.empty()) {
      return make_unexpected(make_error_code(errc::network_unreachable));
    }
    aws_.UpdateRegion(std_sv(new_region));
    ec = ConnectInternal();
    host = http_client_->host();

    VLOG(1) << "Redirecting to " << host;

    req.set(h2::field::host, host);

    ec = SendRequest(&aws_, http_client_.get(), &req, &resp);
    if (ec)
      return make_unexpected(ec);
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "http error: " << resp;
    return make_unexpected(make_error_code(errc::inappropriate_io_control_operation));
  }

  if (!absl::StartsWith(resp.body(), "<?xml"))
    return make_unexpected(make_error_code(errc::bad_message));

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

error_code S3Bucket::ListAllObjects(string_view bucket_path, ListObjectCb cb) {
  std::string marker = "";
  unsigned max_keys = kAwsMaxKeys;
  do {
    auto res = ListObjects(bucket_path, cb, marker, max_keys);
    if (!res)
      return res.error();

    marker = res.value();
  } while (!marker.empty());

  return std::error_code{};
}

io::Result<io::ReadonlyFile*> S3Bucket::OpenReadFile(string_view path,
                                                     const io::ReadonlyFile::Options& opts) {
  return OpenS3ReadFile(path, &aws_, http_client_.get(), opts);
}

string S3Bucket::GetHost() const {
  if (!endpoint_.empty())
    return endpoint_;

  return absl::StrCat("s3.", aws_.region(), ".amazonaws.com");
}

error_code S3Bucket::ConnectInternal() {
  string host = GetHost();
  auto pos = host.rfind(':');
  string port;

  if (pos != string::npos) {
    port = host.substr(pos + 1);
    host = host.substr(0, pos);
  } else {
    port = "80";
  }

  if (IsAwsEndpoint(host))
    host = absl::StrCat(bucket_, ".", host);

  VLOG(1) << "Connecting to " << host << ":" << port;
  return http_client_->Connect(host, port);
}

}  // namespace cloud
}  // namespace util
