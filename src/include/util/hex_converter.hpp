//===----------------------------------------------------------------------===//
//                         Scrooge
//
// util/hex_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
} // namespace scrooge
} // namespace duckdb
