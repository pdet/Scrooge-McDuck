#include "functions/scanner.hpp"
#include "duckdb/common/helper.hpp"
#include <iostream>
#include <sstream>
#include <string>
#include "json.hpp"
#include "util/hex_converter.hpp"
#include "util/eth_maps.hpp"
#include "util/http_util.hpp"
#include "util/eth_uniswap_map.hpp"

namespace duckdb {
namespace scrooge {
class EthGetLogsRequest : public TableFunctionData {
public:
  // Constructor to initialize the JSON-RPC request with given parameters
  EthGetLogsRequest(uint32_t id, string address, string topic,
                    int64_t from_block_p, int64_t to_block_p,
                    int64_t blocks_per_thread_p, string rpc_url_p,
                    const bool strict_p)
      : id(id), address(std::move(address)), topic(std::move(topic)),
        from_block(from_block_p), to_block(to_block_p),
        blocks_per_thread(blocks_per_thread_p), rpc_url(std::move(rpc_url_p)),
        strict((strict_p)) {}

  const uint32_t id;
  const string address;
  const string topic;
  const idx_t from_block;
  const idx_t to_block;
  const int64_t blocks_per_thread;
  const string rpc_url;
  const bool strict;
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
  bool strict = false;

  for (auto &kv : input.named_parameters) {
    auto loption = StringUtil::Lower(kv.first);
    if (loption == "blocks_per_thread") {
      blocks_per_thread = kv.second.GetValue<int64_t>();
      if (blocks_per_thread < -1 || blocks_per_thread == 0) {
        throw InvalidInputException(
            "blocks_per_thread must be higher than 0 or equal to -1. -1 means "
            "one thread will read all the blocks");
      }
    } else if (loption == "strict") {
      strict = kv.second.GetValue<bool>();
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
    if (token_addresses.find(address) == token_addresses.end()) {
      if (uniswap_addresses.find(address) == uniswap_addresses.end()) {
        vector<string> candidates;
        if (address.size() < 8) {
          candidates = StringUtil::TopNLevenshtein(token_symbols, address);
        } else {
          candidates = StringUtil::TopNLevenshtein(uniswap_symbols, address);
        }
        std::ostringstream error;
        error << "Failed to infer the address \"" << address << "\"";
        error << ". Address must be either a hex (e.g., 0x...) or a valid "
                 "token symbol.\n";
        error << "Suggested token symbols: \n";
        for (idx_t candidate_idx = 0; candidate_idx < candidates.size();
             candidate_idx++) {
          error << candidates[candidate_idx];
          if (candidate_idx < candidates.size() - 1) {
            error << ", ";
          }
        }
        throw InvalidInputException(error.str());
      } else {
        address = uniswap_addresses.at(address);
      }
    } else {
      address = token_addresses.at(address);
    }
  }

  if (!(topic.size() >= 2 && topic.substr(0, 2) == "0x")) {
    transform(topic.begin(), topic.end(), topic.begin(), ::toupper);
    if (event_to_hex_signatures.find(topic) == event_to_hex_signatures.end()) {
      std::ostringstream error;
      error << "Failed to infer the topic address \"" << topic << "\"";
      error << ". Address must be either a hex (e.g., 0x...) or a valid topic "
               "string.\n";
      error << "Suggested topic: "
            << StringUtil::TopNLevenshtein(event_strings, topic, 1).back();

      throw InvalidInputException(error.str());
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
                                      blocks_per_thread, node_url, strict);
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
  RPCGlobalState(const EthGetLogsRequest &bind_logs_p, idx_t number_of_threads,
                 const vector<idx_t> projection_ids_p)
      : bind_logs(bind_logs_p), system_threads(number_of_threads),
        projection_ids(projection_ids_p) {
    state.start = bind_logs.from_block;
    if (bind_logs.blocks_per_thread == -1) {
      state.end = bind_logs.to_block;
    } else {
      state.end = bind_logs.blocks_per_thread >
                          bind_logs.to_block - bind_logs.from_block
                      ? bind_logs.to_block
                      : bind_logs.from_block + bind_logs.blocks_per_thread;
    }
    finished = 0;
  }

  unique_ptr<RCPRequest> Next(bool init) {
    CurrentState cur_state;
    idx_t cur_request_id;
    {
      lock_guard<mutex> parallel_lock(main_mutex);
      if (state.start > bind_logs.to_block) {
        ++finished;
        return nullptr;
      }
      if (!init) {
        ++finished;
      }
      cur_state = state;
      // we start off one position after the end
      state.start = state.end + 1;
      if (bind_logs.blocks_per_thread != -1) {
        if (bind_logs.blocks_per_thread > bind_logs.to_block - state.start) {
          state.end = bind_logs.to_block;
        } else {
          state.end = state.start + bind_logs.blocks_per_thread;
        }
      }
      cur_request_id = GetRequestId();
    }
    return make_uniq<RCPRequest>(bind_logs, cur_request_id, cur_state);
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
  std::atomic<idx_t> finished;
  const vector<idx_t> projection_ids;
};

unique_ptr<GlobalTableFunctionState>
EthRPC::InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<EthGetLogsRequest>();
  return make_uniq<RPCGlobalState>(bind_data, context.db->NumberOfThreads(),
                                   input.column_ids);
}

unique_ptr<LocalTableFunctionState>
EthRPC::InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                  GlobalTableFunctionState *global_state_p) {
  if (!global_state_p) {
    return nullptr;
  }
  auto &global_state = global_state_p->Cast<RPCGlobalState>();

  return make_uniq<RPCLocalState>(global_state.Next(true));
}

double EthRPC::ProgressBar(ClientContext &context,
                           const FunctionData *bind_data_p,
                           const GlobalTableFunctionState *global_state) {
  if (!global_state) {
    return 0;
  }
  auto &bind_data = bind_data_p->Cast<EthGetLogsRequest>();
  auto &data = global_state->Cast<RPCGlobalState>();
  double percentage = (double)(data.finished) *
                      (double)bind_data.blocks_per_thread /
                      (double)(bind_data.to_block - bind_data.from_block);
  return percentage * 100;
}

void EthRPC::Scan(ClientContext &context, TableFunctionInput &data_p,
                  DataChunk &output) {

  auto &local_state = (RPCLocalState &)*data_p.local_state;
  auto &global_state = (RPCGlobalState &)*data_p.global_state;
  auto &bind_data = data_p.bind_data->Cast<EthGetLogsRequest>();
  if (!local_state.rpc_request) {
    // We are done
    return;
  }

  if (local_state.rpc_request->done) {
    local_state.rpc_request = global_state.Next(false);
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

  for (idx_t row_idx = 0; row_idx < cur_chunk_size; row_idx++) {
    auto &cur_result_row = result[rpc_request.cur_row++];
    for (idx_t col_idx = 0; col_idx < global_state.projection_ids.size();
         col_idx++) {
      vector<string> topics = cur_result_row["topics"];
      idx_t event_type = 6;
      if (event_signatures.find(topics[0]) != event_signatures.end()) {
        event_type = event_signatures.at(topics[0]).id;
      }
      switch (global_state.projection_ids[col_idx]) {
      case 0:
        // Column 0 - Address
        ((string_t *)output.data[col_idx].GetData())[row_idx] =
            StringVector::AddString(output.data[col_idx],
                                    cur_result_row["address"].dump());
        break;
      case 1:
        // Column 1 - Event Type
        ((uint8_t *)output.data[col_idx].GetData())[row_idx] = event_type;
        break;
      case 2:
        // Column 2 - Block Hash
        ((string_t *)output.data[col_idx].GetData())[row_idx] =
            StringVector::AddString(output.data[col_idx],
                                    cur_result_row["blockHash"].dump());
        break;
      case 3:
        // Column 3 - Block Number
        ((uint32_t *)output.data[col_idx].GetData())[row_idx] =
            stoi(cur_result_row["blockNumber"].get<string>(), nullptr, 16);
        break;
      case 4:
        // Column 4 - Data
        {
          uhugeint_t u_hugeint_res{};
          auto &child_vector = ListVector::GetEntry(output.data[col_idx]);
          auto list_content = FlatVector::GetData<uhugeint_t>(child_vector);
          auto current_list_size =
              ListVector::GetListSize(output.data[col_idx]);
          auto current_list_capacity =
              ListVector::GetListCapacity(output.data[col_idx]);

          auto result_data =
              FlatVector::GetData<list_entry_t>(output.data[col_idx]);
          auto &list_entry = result_data[row_idx];
          list_entry.offset = current_list_size;
          auto &child_validity = FlatVector::Validity(child_vector);
          if (event_type == 2) {
            // Sync Event
            std::string data = cur_result_row["data"];
            std::string reserve0_hex = data.substr(2, 64);
            std::string reserve1_hex = data.substr(66, 64);
            // Make sure we have enough room for the new entries
            if (current_list_size + 2 >= current_list_capacity) {
              ListVector::Reserve(output.data[col_idx],
                                  current_list_capacity * 2);
              list_content = FlatVector::GetData<uhugeint_t>(child_vector);
            }

            if (!HexConverter::HexToUhugeiInt(reserve0_hex, bind_data.strict,
                                              u_hugeint_res)) {
              child_validity.SetInvalid(current_list_size++);
            } else {
              list_content[current_list_size++] = u_hugeint_res;
            }
            if (!HexConverter::HexToUhugeiInt(reserve1_hex, bind_data.strict,
                                              u_hugeint_res)) {
              child_validity.SetInvalid(current_list_size++);
            } else {
              list_content[current_list_size++] = u_hugeint_res;
            }
          } else {
            if (current_list_size + 1 >= current_list_capacity) {
              ListVector::Reserve(output.data[col_idx],
                                  current_list_capacity * 2);
              list_content = FlatVector::GetData<uhugeint_t>(child_vector);
            }
            std::string data = cur_result_row["data"];
            std::string reserve0_hex = data.substr(2);
            if (!HexConverter::HexToUhugeiInt(reserve0_hex, bind_data.strict,
                                              u_hugeint_res)) {
              child_validity.SetInvalid(current_list_size++);
            } else {
              list_content[current_list_size++] = u_hugeint_res;
            }
          }
          list_entry.length = current_list_size - list_entry.offset;
          ListVector::SetListSize(output.data[col_idx], current_list_size);
          break;
        }
      case 5:
        ((uint32_t *)output.data[col_idx].GetData())[row_idx] =
            stoi(cur_result_row["logIndex"].get<string>(), nullptr, 16);
        break;
      case 6:
        ((bool *)output.data[col_idx].GetData())[row_idx] =
            (int8_t)cur_result_row["removed"];
        break;
      case 7: {
        // Column 7 - Topics
        auto &child_vector = ListVector::GetEntry(output.data[col_idx]);
        auto list_content = FlatVector::GetData<string_t>(child_vector);
        auto current_list_size = ListVector::GetListSize(output.data[col_idx]);
        auto current_list_capacity =
            ListVector::GetListCapacity(output.data[col_idx]);

        auto result_data =
            FlatVector::GetData<list_entry_t>(output.data[col_idx]);
        auto &list_entry = result_data[row_idx];
        list_entry.offset = current_list_size;
        // Make sure we have enough room for the new entries
        if (current_list_size + topics.size() - 1 >= current_list_capacity) {
          ListVector::Reserve(output.data[col_idx], current_list_capacity * 2);
          list_content = FlatVector::GetData<string_t>(child_vector);
        }
        for (idx_t i = 1; i < topics.size(); i++) {
          list_content[current_list_size++] =
              StringVector::AddString(child_vector, topics[i]);
        }
        list_entry.length = current_list_size - list_entry.offset;
        ListVector::SetListSize(output.data[col_idx], current_list_size);

        break;
      }
      case 8:
        // Column 8 - TransactionHash
        ((string_t *)output.data[col_idx].GetData())[row_idx] =
            StringVector::AddString(output.data[col_idx],
                                    cur_result_row["transactionHash"].dump());
        break;
      case 9:
        // Column 9 - Transaction Index
        ((int32_t *)output.data[col_idx].GetData())[row_idx] =
            stoi(cur_result_row["transactionIndex"].get<string>(), nullptr, 16);
        break;
      default:
        break;
      }
    }
  }
  if (result.size() == rpc_request.cur_row) {
    // we are done
    rpc_request.done = true;
  }
}
} // namespace scrooge
} // namespace duckdb
