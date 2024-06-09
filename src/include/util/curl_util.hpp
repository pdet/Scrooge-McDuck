//===----------------------------------------------------------------------===//
//                         Scrooge
//
// util/curl_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
namespace scrooge {

struct CurlUtil {
  // Function to handle the curl response
  static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                              void *userp) {
    ((string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
  }

  static nlohmann::basic_json<> Request(const string &rpc_url,
                                        const string &request) {
    CURL *curl;
    CURLcode res;
    string read_buffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if (curl) {

      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);

      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
      curl_easy_setopt(curl, CURLOPT_URL, rpc_url.c_str());

      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.c_str());

      // Set headers
      struct curl_slist *headers = nullptr;
      headers = curl_slist_append(headers, "Content-Type: application/json");
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

      // Set callback function to handle the response
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &read_buffer);

      // Perform the request
      res = curl_easy_perform(curl);
      if (res != CURLE_OK) {
        string error = "curl_easy_perform() failed: ";
        error = +curl_easy_strerror(res);
        throw std::runtime_error(error);
      }
      // Clean up
      curl_easy_cleanup(curl);
      curl_global_cleanup();
    }
    // Parse the response JSON
    return nlohmann::json::parse(read_buffer);
  }
};

} // namespace scrooge
} // namespace duckdb