#include "functions/scanner.hpp"
#include "duckdb/common/helper.hpp"
#include <curl/curl.h>
#include <iostream>
#include <sstream>
#include <string>
#include "json.hpp"

namespace duckdb {
namespace scrooge {

class HexConverter {
public:
  static uint64_t HexToUBigInt(const string &hex) {
    unsigned long long number = 0;
    std::stringstream ss;
    ss << hex << hex.substr(2);
    ss >> number;
    return number;
  }
  template <class T> static string NumericToHex(T value) {
    std::stringstream stream_to;
    stream_to << "0x" << std::hex << value;
    return stream_to.str();
  }

  static bool IsHex(const string &hex) {
    return hex.size() >= 2 && hex.substr(0, 2) == "0x";
  }
};

class EthGetLogsRequest : public TableFunctionData {
public:
  // Constructor to initialize the JSON-RPC request with given parameters
  EthGetLogsRequest(uint32_t id, string address, string topic,
                    int64_t from_block_p, int64_t to_block_p,
                    int64_t blocks_per_thread_p, string rpc_url_p)
      : id(id), address(move(address)), topic(move(topic)),
        from_block(from_block_p), to_block(to_block_p),
        blocks_per_thread(blocks_per_thread_p), rpc_url(move(rpc_url_p)) {}

  const uint32_t id;
  const string address;
  const string topic;
  const idx_t from_block;
  const idx_t to_block;
  const int64_t blocks_per_thread;
  const string rpc_url;
};

// Function to handle the curl response
size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

// Event data structure
struct Event {
  string name;
  vector<string> parameterTypes;
  uint8_t id;
};

// Known event signatures and their descriptions
unordered_map<string, Event> event_signatures = {
    {"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
     {"Transfer", {"address", "address", "uint256"}, 0}},
    {"0x8c5be1e5ebec7d5bd14f714f0f3a56d4af4fef1d7f7bb78a0c69d4cdb365d97e",
     {"Approval", {"address", "address", "uint256"}, 1}},
    {"0x1c411e9a96aaffc4e1b98a00ef4a86ce2e058f7d78f2c83971e57f36d39188c7",
     {"Sync", {"uint112", "uint112"}, 2}},
    {"0x4a39dc06d4c0dbc64b70b7bfa42387d156a1b455ad1583a19957f547256c22e5",
     {"TransferSingle",
      {"address", "address", "address", "uint256", "uint256"},
      3}},
    {"0xc3d58168cbe0a160b82265397fa6b10ff1c847f6b8df03e1d36e5e4bba925ed5",
     {"TransferBatch",
      {"address", "address", "address", "uint256[]", "uint256[]"},
      4}},
    {"0x17307eab39c17eae00a0c514c4b17d95e15ee86d924b1cf3b9c9dc7f59e3e5a1",
     {"ApprovalForAll", {"address", "address", "bool"}, 5}}};

unordered_map<string, string> token_adresses = {
    {"ETH", "0x0000000000000000000000000000000000000000"},  // Ether
    {"USDT", "0xdAC17F958D2ee523a2206206994597C13D831ec7"}, // Tether USD
    {"USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606EB48"}, // USD Coin
    {"DAI", "0x6B175474E89094C44Da98b954EedeAC495271d0F"},  // DAI Stablecoin
    {"BNB", "0xB8c77482e45F1F44dE1745F52C74426C631bDD52"},  // Binance Coin
    {"UNI", "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984"},  // Uniswap
    {"LINK", "0x514910771AF9Ca656af840dff83E8264EcF986CA"}, // Chainlink
    {"WBTC", "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"}, // Wrapped Bitcoin
    {"LTC", "0x4338665CBB7B2485A8855A139b75D5e34AB0DB94"},  // Litecoin
    {"BUSD", "0x4fabb145d64652a948d72533023f6e7a623c7c53"}  // Binance USD
};

unordered_map<string, string> event_to_hex_signatures = {
    {"TRANSFER",
     "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
    {"APPROVAL",
     "0x8c5be1e5ebec7d5bd14f714f13107d2b7b6e6d2027d8710d3b0b4f43363d9ab8"},
    {"DEPOSIT",
     "0xe1fffcc4923d54a4bdb6e0a28d0b6b7b2fb29a260555b24c75f6b1e7b3bfb123"},
    {"WITHDRAWAL",
     "0x7fcf26fc5cc6d7e6e05e1144b4b68871c514e020f1fc0d7d839bd09f61a8bc86"}};

unordered_map<string, uint8_t> event_to_enum = {
    {"Transfer", 0},
    {"Approval", 1},
    {"Sync", 2},
    {"Transfer", 0},
};

unique_ptr<FunctionData> EthRPC::Bind(ClientContext &context,
                                      TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types,
                                      vector<string> &names) {
  // Get Arguments
  auto address = input.inputs[0].GetValue<string>();
  auto topic = input.inputs[1].GetValue<string>();
  auto from_block = input.inputs[2].GetValue<int64_t>();
  auto to_block = input.inputs[3].GetValue<int64_t>();
  int64_t blocks_per_thread = -1;

  for (auto &kv : input.named_parameters) {
    auto loption = StringUtil::Lower(kv.first);
    if (loption == "blocks_per_thread") {
      blocks_per_thread = kv.second.GetValue<int64_t>();
      if (blocks_per_thread < -1 || blocks_per_thread == 0) {
        throw InvalidInputException(
            "blocks_per_thread must be higher than 0 or equal to -1. -1 means "
            "one thread will read all the blocks");
      }
    } else {
      throw BinderException(
          "Unrecognized function name \"%s\" for read_eth definition",
          kv.first);
    }
  }

  if (from_block < 0) {
    throw InvalidInputException("FromBlock must be higher or equal to 0");
  }

  if (!(address.size() >= 2 && address.substr(0, 2) == "0x")) {
    transform(address.begin(), address.end(), address.begin(), ::toupper);
    if (token_adresses.find(address) == token_adresses.end()) {
      throw InvalidInputException(
          "Address must be either a hex or a valid token string");
    }
    address = token_adresses[address];
  }

  if (!(topic.size() >= 2 && topic.substr(0, 2) == "0x")) {
    transform(topic.begin(), topic.end(), topic.begin(), ::toupper);
    if (event_to_hex_signatures.find(topic) == event_to_hex_signatures.end()) {
      throw InvalidInputException(
          "Event must be either a hex or a valid token string");
    }
    topic = event_to_hex_signatures[topic];
  }

  //   address: Contract address emitting the log.
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("address");
  //   event_type: Contract address emitting the log.
  string enum_name = "ETH_EVENT";
  Vector order_errors(LogicalType::VARCHAR, 7);
  order_errors.SetValue(0, "Transfer");
  order_errors.SetValue(1, "Approval");
  order_errors.SetValue(2, "Sync");
  order_errors.SetValue(3, "TransferSingle");
  order_errors.SetValue(4, "TransferBatch");
  order_errors.SetValue(5, "ApprovalForAll");
  order_errors.SetValue(6, "Unknown");
  LogicalType enum_type = LogicalType::ENUM(enum_name, order_errors, 7);
  return_types.emplace_back(enum_type);
  names.emplace_back("event_type");
  // blockHash: Hash of the block containing the log.
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("block_hash");
  // blockNumber: Number of the block containing the log.
  return_types.emplace_back(LogicalType::INTEGER);
  names.emplace_back("block_number");
  // data: Event-specific data (e.g., amount transferred).
  return_types.emplace_back(LogicalType::UBIGINT);
  names.emplace_back("data");
  // logIndex: Log's position within the block.
  return_types.emplace_back(LogicalType::UINTEGER);
  names.emplace_back("log_index");
  // removed: Indicates if the log was removed in a chain reorganization.
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("removed");
  // topics: Indexed event parameters (e.g., event signature, sender, and
  // receiver addresses).
  return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
  names.emplace_back("topics");
  // transactionHash: Hash of the transaction generating the log.
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("transaction_hash");
  // transactionIndex: Transaction's position within the block.
  return_types.emplace_back(LogicalType::INTEGER);
  names.emplace_back("transaction_index");

  Value result;
  string key = "eth_node_url";
  context.TryGetCurrentSetting(key, result);
  auto node_url = result.GetValue<string>();

  return make_uniq<EthGetLogsRequest>(0, address, topic, from_block, to_block,
                                      blocks_per_thread, node_url);
}

struct CurrentState {
  uint32_t start{};
  uint32_t end{};
};

struct RCPRequest {
  explicit RCPRequest(const EthGetLogsRequest &bind_logs_p, idx_t request_id_p,
                      CurrentState &state_p)
      : bind_logs(bind_logs_p), request_id(request_id_p), state(state_p) {

    // Let's do the RPC and parse the result
    // Get JSON formatted string
    string request = ToString();

    CURL *curl;
    CURLcode res;
    string read_buffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if (curl) {

      curl_easy_setopt(curl, CURLOPT_URL, bind_logs.rpc_url.c_str());

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
    json = nlohmann::json::parse(read_buffer);
    if (!json.contains("result")) {
      // This is funky, we should error
      throw std::runtime_error("JSON Error: " + json.dump());
    }
  }

  // Method to return the JSON request as a string
  string ToString() const {
    std::ostringstream oss;
    oss << "{"
        << R"("jsonrpc":"2.0",)"
        << "\"id\":" << request_id << ","
        << R"("method":"eth_getLogs",)"
        << "\"params\":[{"
        << R"("address":")" << bind_logs.address << "\",";
    if (!bind_logs.topic.empty()) {
      oss << R"("topics":[")" << bind_logs.topic << "\"],";
    }
    oss << R"("fromBlock":")" << HexConverter::NumericToHex(state.start)
        << "\","
        << R"("toBlock":")" << HexConverter::NumericToHex(state.end) << "\""
        << "}]"
        << "}";
    return oss.str();
  }

  const EthGetLogsRequest &bind_logs;
  idx_t request_id;
  CurrentState state;

  nlohmann::basic_json<> json;
  idx_t cur_row = 0;
  bool done = false;
};

struct RPCLocalState : public LocalTableFunctionState {
  explicit RPCLocalState(unique_ptr<RCPRequest> rpc_request_p)
      : rpc_request(move(rpc_request_p)) {}

  unique_ptr<RCPRequest> rpc_request;
};

//! Global State
struct RPCGlobalState : public GlobalTableFunctionState {
  RPCGlobalState(const EthGetLogsRequest &bind_logs_p, idx_t number_of_threads)
      : bind_logs(bind_logs_p), system_threads((number_of_threads)) {
    state.start = bind_logs.from_block;
    if (bind_logs.blocks_per_thread == -1) {
      state.end = bind_logs.to_block;
    } else {
      state.end = bind_logs.blocks_per_thread >
                          bind_logs.to_block - bind_logs.from_block
                      ? bind_logs.to_block
                      : bind_logs.from_block + bind_logs.blocks_per_thread;
    }
  }

  unique_ptr<RCPRequest> Next() {
    lock_guard<mutex> parallel_lock(main_mutex);

    if (state.start > bind_logs.to_block) {
      return nullptr;
    }
    auto cur_state = state;
    // we start off one position after the end
    state.start = state.end + 1;
    if (bind_logs.blocks_per_thread != -1) {
      if (bind_logs.blocks_per_thread > bind_logs.to_block - state.start) {
        state.end = bind_logs.to_block;
      } else {
        state.end = state.start + bind_logs.blocks_per_thread;
      }
    }

    return make_uniq<RCPRequest>(bind_logs, GetRequestId(), cur_state);
  }

  idx_t GetRequestId() { return request_id++; }

  idx_t MaxThreads() const override {
    idx_t thread_iterations = (bind_logs.to_block - bind_logs.from_block) /
                              bind_logs.blocks_per_thread;
    if (system_threads < thread_iterations) {
      return system_threads;
    }
    return thread_iterations;
  }

  const EthGetLogsRequest &bind_logs;
  const idx_t system_threads;
  CurrentState state;
  mutable mutex main_mutex;
  idx_t request_id = 0;
};

unique_ptr<GlobalTableFunctionState>
EthRPC::InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<EthGetLogsRequest>();
  return make_uniq<RPCGlobalState>(bind_data, context.db->NumberOfThreads());
}

unique_ptr<LocalTableFunctionState>
EthRPC::InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                  GlobalTableFunctionState *global_state_p) {
  if (!global_state_p) {
    return nullptr;
  }
  auto &global_state = global_state_p->Cast<RPCGlobalState>();

  return make_uniq<RPCLocalState>(global_state.Next());
}

void EthRPC::Scan(ClientContext &context, TableFunctionInput &data_p,
                  DataChunk &output) {

  auto &local_state = (RPCLocalState &)*data_p.local_state;

  if (!local_state.rpc_request) {
    // We are done
    return;
  }

  if (local_state.rpc_request->done) {
    auto &global_state = (RPCGlobalState &)*data_p.global_state;
    local_state.rpc_request = global_state.Next();
    if (!local_state.rpc_request) {
      // We are done
      return;
    }
  }
  auto &rpc_request = *local_state.rpc_request;
  auto &result = rpc_request.json["result"];
  idx_t cur_chunk_size =
      result.size() - rpc_request.cur_row > STANDARD_VECTOR_SIZE
          ? STANDARD_VECTOR_SIZE
          : result.size() - rpc_request.cur_row;
  output.SetCardinality(cur_chunk_size);
  auto address_column = (string_t *)output.data[0].GetData();
  auto event_type = (uint8_t *)output.data[1].GetData();

  auto block_hash = (string_t *)output.data[2].GetData();
  auto block_number = (uint32_t *)output.data[3].GetData();
  auto data = (uint64_t *)output.data[4].GetData();
  auto log_index = (uint32_t *)output.data[5].GetData();
  auto removed = (bool *)output.data[6].GetData();

  auto transaction_hash = (string_t *)output.data[8].GetData();
  auto transaction_index = (int32_t *)output.data[9].GetData();

  for (idx_t row_idx = 0; row_idx < cur_chunk_size; row_idx++) {
    auto &cur_result_row = result[rpc_request.cur_row++];

    // Column 0 - Address
    address_column[row_idx] = StringVector::AddString(
        output.data[0], cur_result_row["address"].dump());

    // Column 1 - Event Type
    vector<string> topics = cur_result_row["topics"];
    // string event_type;
    if (event_signatures.find(topics[0]) == event_signatures.end()) {
      event_type[row_idx] = 6;
    } else {
      event_type[row_idx] = event_signatures[topics[0]].id;
    }

    // Column 2 - Block Hash
    block_hash[row_idx] = StringVector::AddString(
        output.data[2], cur_result_row["blockHash"].dump());

    // Column 3 - Block Number
    block_number[row_idx] =
        stoi(cur_result_row["blockNumber"].get<string>(), nullptr, 16);

    // Column 4 - Data
    // Fixme: we need to convert this hex differently
    data[row_idx] = HexConverter::HexToUBigInt(cur_result_row["data"]);

    // Column 5 - LogIndex
    log_index[row_idx] =
        stoi(cur_result_row["logIndex"].get<string>(), nullptr, 16);

    // Column 6 - Removed
    removed[row_idx] = (int8_t)cur_result_row["removed"];

    // Column 7 - Topics
    vector<Value> values;
    for (idx_t i = 1; i < topics.size(); i++) {
      values.emplace_back(topics[i]);
    }
    output.SetValue(7, row_idx, Value::LIST(values));

    // Column 8 - TransactionHash
    transaction_hash[row_idx] = StringVector::AddString(
        output.data[8], cur_result_row["transactionHash"].dump());

    // Column 9 - Transaction Index
    transaction_index[row_idx] =
        stoi(cur_result_row["transactionIndex"].get<string>(), nullptr, 16);
  }
  if (result.size() == rpc_request.cur_row) {
    // we are done
    rpc_request.done = true;
  }
}
} // namespace scrooge
} // namespace duckdb
