//===----------------------------------------------------------------------===//
//                         Scrooge
//
// util/http_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace duckdb {
namespace scrooge {

class HTTPUtil {
public:
  static duckdb_httplib_openssl::Result Request(const std::string &url,
                                                const std::string &request) {
    std::string host, path;
    bool use_ssl = false;
    string clean_url = url;
    // Find the protocol
    size_t protocol_end = url.find("://");
    if (protocol_end != std::string::npos) {
      std::string protocol = url.substr(0, protocol_end);
      if (protocol == "https") {
        use_ssl = true;
      } else if (protocol != "http") {
        throw std::runtime_error("Unsupported protocol: " + protocol);
      }
      clean_url = url.substr(protocol_end + 3);
    }

    // Split the host and path
    size_t path_start = clean_url.find('/');
    if (path_start != std::string::npos) {
      host = clean_url.substr(0, path_start);
      path = clean_url.substr(path_start);
    } else {
      host = clean_url;
      path = "/";
    }

    // Create an HTTP or HTTPS client based on the protocol
    // std::unique_ptr<duckdb_httplib_openssl::Client> client;
    if (use_ssl) {
      auto client = make_uniq<duckdb_httplib_openssl::SSLClient>(host);
      // Perform the HTTP POST request
      auto res = client->Post(path.c_str(), request, "application/json");

      // Check if the request was successful
      if (!res || res->status != 200) {
        throw std::runtime_error("HTTP Request failed with status: " +
                                 std::to_string(res ? res->status : -1));
      }
      return res;
      // dynamic_cast<duckdb_httplib_openssl::SSLClient*>(client.get())->enable_server_certificate_verification(false);
    } else {
      auto client = make_uniq<duckdb_httplib_openssl::Client>(host);
      // Perform the HTTP POST request
      auto res = client->Post(path.c_str(), request, "application/json");

      // Check if the request was successful
      if (!res || res->status != 200) {
        throw std::runtime_error("HTTP Request failed with status: " +
                                 std::to_string(res ? res->status : -1));
      }
      return res;
    }
  }
};

} // namespace scrooge
} // namespace duckdb
