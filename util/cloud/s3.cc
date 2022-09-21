// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/s3.h"

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "base/logging.h"
#include "util/cloud/aws.h"

namespace util {
namespace cloud {

using namespace std;
namespace h2 = boost::beast::http;
using nonstd::make_unexpected;

const char kRootDomain[] = "s3.amazonaws.com";

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
}  // namespace cloud
}  // namespace util
