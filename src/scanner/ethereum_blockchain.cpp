#include "functions/scanner.hpp"
#include "duckdb/common/helper.hpp"
#include <iostream>
#include <sstream>
#include <string>
#include "json.hpp"
#include "util/hex_converter.hpp"
#include "util/eth_maps.hpp"
#include "util/http_util.hpp"

namespace duckdb {
namespace scrooge {
class EthGetLogsRequest : public TableFunctionData {
public:
  // Constructor to initialize the JSON-RPC request with given parameters
  EthGetLogsRequest(uint32_t id, string address, string topic,
                    int64_t from_block_p, int64_t to_block_p,
                    int64_t blocks_per_thread_p, string rpc_url_p)
      : id(id), address(std::move(address)), topic(std::move(topic)),
        from_block(from_block_p), to_block(to_block_p),
        blocks_per_thread(blocks_per_thread_p), rpc_url(std::move(rpc_url_p)) {}

  const uint32_t id;
  const string address;
  const string topic;
  const idx_t from_block;
  const idx_t to_block;
  const int64_t blocks_per_thread;
  const string rpc_url;
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
    address = token_adresses.at(address);
  }

  if (!(topic.size() >= 2 && topic.substr(0, 2) == "0x")) {
    transform(topic.begin(), topic.end(), topic.begin(), ::toupper);
    if (event_to_hex_signatures.find(topic) == event_to_hex_signatures.end()) {
      throw InvalidInputException(
          "Event must be either a hex or a valid token string");
    }
    topic = event_to_hex_signatures.at(topic);
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
  return_types.emplace_back(LogicalType::LIST(LogicalType::UHUGEINT));
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

    // Convert the request to a JSON formatted string
    std::string request = ToString();
    std::string url = bind_logs.rpc_url;

    // Perform the HTTP POST request
    auto res = HTTPUtil::Request(url, request);

    // Parse the response JSON
    json = nlohmann::json::parse(res->body);
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
      : rpc_request(std::move(rpc_request_p)) {}

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
      event_type[row_idx] = event_signatures.at(topics[0]).id;
    }

    // Column 2 - Block Hash
    block_hash[row_idx] = StringVector::AddString(
        output.data[2], cur_result_row["blockHash"].dump());

    // Column 3 - Block Number
    block_number[row_idx] =
        stoi(cur_result_row["blockNumber"].get<string>(), nullptr, 16);

    // Column 4 - Data
    // Fixme: we need to convert this hex differently
    vector<Value> data_values;
    if (event_type[row_idx] == 2) {
      // Sync Event
      std::string data = cur_result_row["data"];
      std::string reserve0_hex = data.substr(2, 64);
      std::string reserve1_hex = data.substr(66, 64);
      data_values.emplace_back(
          Value::UHUGEINT(HexConverter::HexToUhugeiInt(reserve0_hex)));
      data_values.emplace_back(
          Value::UHUGEINT(HexConverter::HexToUhugeiInt(reserve1_hex)));
    } else {
      std::string data = cur_result_row["data"];
      std::string reserve0_hex = data.substr(2);
      data_values.emplace_back(
          Value::UHUGEINT(HexConverter::HexToUhugeiInt(reserve0_hex)));
    }
    output.SetValue(4, row_idx, Value::LIST(data_values));

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
    if (!values.empty()) {
      output.SetValue(7, row_idx, Value::LIST(values));
    } else {
      output.SetValue(7, row_idx, Value::EMPTYLIST(LogicalType::VARCHAR));
    }
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
