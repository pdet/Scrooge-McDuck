#include "functions/scanner.hpp"
#include "duckdb/common/helper.hpp"
#include <curl/curl.h>
#include <iostream>
#include <sstream>
#include <string>
#include "json.hpp"

namespace scrooge {
class EthGetLogsRequest : public duckdb::TableFunctionData {
public:
  // Constructor to initialize the JSON-RPC request with given parameters
  EthGetLogsRequest(uint32_t id, const std::string &address,
                    const std::string &topic, const std::string &fromBlock,
                    const std::string &toBlock)
      : id(id), address(address), topic(topic), fromBlock(fromBlock),
        toBlock(toBlock) {}

  // Method to return the JSON request as a string
  std::string ToString() const {
    std::ostringstream oss;
    oss << "{"
        << R"("jsonrpc":"2.0",)"
        << "\"id\":" << id << ","
        << R"("method":"eth_getLogs",)"
        << "\"params\":[{"
        << R"("address":")" << address << "\","
        << R"("topics":[")" << topic << "\"],"
        << R"("fromBlock":")" << fromBlock << "\","
        << R"("toBlock":")" << toBlock << "\""
        << "}]"
        << "}";
    return oss.str();
  }
  bool done = false;

private:
  uint32_t id;
  std::string address;
  std::string topic;
  std::string fromBlock;
  std::string toBlock;
};

// Function to handle the curl response
size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

unsigned long long hexToNumber(const std::string &hex) {
  unsigned long long number = 0;
  std::stringstream ss;
  ss << std::hex << hex.substr(2);
  ss >> number;
  return number;
}

// Event data structure
struct Event {
  std::string name;
  std::vector<std::string> parameterTypes;
};

// Known event signatures and their descriptions
std::unordered_map<std::string, Event> event_signatures = {
    {"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
     {"Transfer", {"address", "address", "uint256"}}},
    {"0x8c5be1e5ebec7d5bd14f714f0f3a56d4af4fef1d7f7bb78a0c69d4cdb365d97e",
     {"Approval", {"address", "address", "uint256"}}},
    {"0x1c411e9a96aaffc4e1b98a00ef4a86ce2e058f7d78f2c83971e57f36d39188c7",
     {"Sync", {"uint112", "uint112"}}},
    {"0x4a39dc06d4c0dbc64b70b7bfa42387d156a1b455ad1583a19957f547256c22e5",
     {"TransferSingle",
      {"address", "address", "address", "uint256", "uint256"}}},
    {"0xc3d58168cbe0a160b82265397fa6b10ff1c847f6b8df03e1d36e5e4bba925ed5",
     {"TransferBatch",
      {"address", "address", "address", "uint256[]", "uint256[]"}}},
    {"0x17307eab39c17eae00a0c514c4b17d95e15ee86d924b1cf3b9c9dc7f59e3e5a1",
     {"ApprovalForAll", {"address", "address", "bool"}}},
    {"0x8c5be1e5ebec7d5bd14f714f0f3a56d4af4fef1d7f7bb78a0c69d4cdb365d97e",
     {"Approval", {"address", "address", "uint256"}}}
    // Add more event signatures here as needed
};

duckdb::unique_ptr<duckdb::FunctionData>
EthRPC::Bind(duckdb::ClientContext &context,
             duckdb::TableFunctionBindInput &input,
             duckdb::vector<duckdb::LogicalType> &return_types,
             duckdb::vector<duckdb::string> &names) {
  // Get Arguments
  auto adress = input.inputs[0].GetValue<std::string>();
  auto topic = input.inputs[1].GetValue<std::string>();
  auto from_block = input.inputs[2].GetValue<std::string>();
  auto to_block = input.inputs[3].GetValue<std::string>();

  //   address: Contract address emitting the log.
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("address");
  //   event_type: Contract address emitting the log.
  std::string enum_name = "ETH_EVENT";
  duckdb::Vector order_errors(duckdb::LogicalType::VARCHAR, 7);
  order_errors.SetValue(0, "Transfer");
  order_errors.SetValue(1, "Approval");
  order_errors.SetValue(2, "Sync");
  order_errors.SetValue(3, "TransferSingle");
  order_errors.SetValue(4, "TransferBatch");
  order_errors.SetValue(5, "ApprovalForAll");
  order_errors.SetValue(6, "Unknown");
  duckdb::LogicalType enum_type = duckdb::LogicalType::ENUM(enum_name, order_errors, 7);
  return_types.emplace_back(enum_type);
  names.emplace_back("event_type");
  // blockHash: Hash of the block containing the log.
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("block_hash");
  // blockNumber: Number of the block containing the log.
  return_types.emplace_back(duckdb::LogicalType::INTEGER);
  names.emplace_back("block_number");
  // data: Event-specific data (e.g., amount transferred).
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("data");
  // logIndex: Log's position within the block.
  return_types.emplace_back(duckdb::LogicalType::INTEGER);
  names.emplace_back("log_index");
  // removed: Indicates if the log was removed in a chain reorganization.
  return_types.emplace_back(duckdb::LogicalType::BOOLEAN);
  names.emplace_back("removed");
  // topics: Indexed event parameters (e.g., event signature, sender, and
  // receiver addresses).
  return_types.emplace_back(
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));
  names.emplace_back("topics");
  // transactionHash: Hash of the transaction generating the log.
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("transaction_hash");
  // transactionIndex: Transaction's position within the block.
  return_types.emplace_back(duckdb::LogicalType::INTEGER);
  names.emplace_back("transaction_index");

  return duckdb::make_uniq<EthGetLogsRequest>(0, adress, topic, from_block,
                                              to_block);
}
void EthRPC::Scan(duckdb::ClientContext &context,
                  duckdb::TableFunctionInput &data_p,
                  duckdb::DataChunk &output) {

  auto &rpc_request = (EthGetLogsRequest &)*data_p.bind_data;

  if (rpc_request.done) {
    return;
  }

  // Get JSON formatted string
  std::string request = rpc_request.ToString();

  CURL *curl;
  CURLcode res;
  std::string read_buffer;

  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();
  if (curl) {
    // auto node_url =
    // context.db->config.options.set_variables["eth_node_url"].GetValue<std::string>();
    std::string node_url =
        "https://mainnet.infura.io/v3/524a429f885f4667b7736ea33cad6be6";
    curl_easy_setopt(curl, CURLOPT_URL, node_url.c_str());

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
      std::string error = "curl_easy_perform() failed: ";
      error = +curl_easy_strerror(res);
      throw std::runtime_error(error);
    }
    // Clean up
    curl_easy_cleanup(curl);
    curl_global_cleanup();
  }
  // Parse the response JSON
  auto response_json = nlohmann::json::parse(read_buffer);
  if (response_json.contains("result")) {
    output.SetCardinality(response_json["result"].size());
    idx_t row_idx = 0;
    for (const auto &log : response_json["result"]) {
      idx_t col_idx = 0;
      std::string address = log["address"];
      output.SetValue(col_idx++, row_idx, {address});
      std::vector<std::string> topics = log["topics"];
      std::string event_type;
      if (event_signatures.find(topics[0]) == event_signatures.end()) {
        event_type = "Unknown";
      } else {
        event_type = event_signatures[topics[0]].name;
      }
      output.SetValue(col_idx++, row_idx, {event_type});
      std::string blockHash = log["blockNumber"];
      output.SetValue(col_idx++, row_idx, {blockHash});
      int blockNumber =
          std::stoi(log["blockNumber"].get<std::string>(), nullptr, 16);
      output.SetValue(col_idx++, row_idx, {blockNumber});
      uint64_t data = hexToNumber(log["data"]);
      output.SetValue(col_idx++, row_idx, duckdb::Value::UBIGINT(data));
      int logIndex = std::stoi(log["logIndex"].get<std::string>(), nullptr, 16);
      output.SetValue(col_idx++, row_idx, {logIndex});
      bool removed = log["removed"];
      output.SetValue(col_idx++, row_idx,
                      duckdb::Value::BOOLEAN((int8_t)removed));

      duckdb::vector<duckdb::Value> values;
      for (idx_t i = 1; i < topics.size(); i++) {
          values.emplace_back(topics[i]);
      }
      output.SetValue(col_idx++, row_idx, duckdb::Value::LIST(values));
      std::string transactionHash = log["transactionHash"];
      output.SetValue(col_idx++, row_idx, transactionHash);
      int transactionIndex =
          std::stoi(log["transactionIndex"].get<std::string>(), nullptr, 16);
      output.SetValue(col_idx, row_idx++, transactionIndex);
    }
  } else {
    throw std::runtime_error("JSON Error");
  }

  rpc_request.done = true;
}
} // namespace scrooge
