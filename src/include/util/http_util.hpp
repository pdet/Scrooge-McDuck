//===----------------------------------------------------------------------===//
//                         Scrooge
//
// util/http_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
namespace scrooge {

class HTTPUtil {
public:
  static duckdb_httplib_openssl::Result Request(string &url,
                                                const string &request) {
    std::string host, path;
    size_t protocol_end = url.find("://");
    if (protocol_end != std::string::npos) {
      url = url.substr(protocol_end + 3);
    }
    size_t path_start = url.find('/');
    if (path_start != std::string::npos) {
      host = url.substr(0, path_start);
      path = url.substr(path_start);
    } else {
      host = url;
      path = "/";
    }

    // Create an HTTP client
    duckdb_httplib_openssl::SSLClient client(host);

    // Set SSL options
    client.enable_server_certificate_verification(false);

    // Perform the HTTP POST request
    auto res = client.Post(path.c_str(), request, "application/json");

    // Check if the request was successful
    if (!res || res->status != 200) {
      throw std::runtime_error("HTTP Request failed with status: " +
                               std::to_string(res ? res->status : -1));
    }
    return res;
  }
};

} // namespace scrooge
} // namespace duckdb