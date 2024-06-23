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
  static uint64_t HexToUBigInt(const string &hex_str) {
    return std::stoull(hex_str, nullptr, 16);
  }
  static uhugeint_t HexToUhugeiInt(const std::string &hex) {
    std::size_t firstNonZero = hex.find_first_not_of('0');
    if (hex.size() - firstNonZero > 32) {
      throw NotImplementedException(
          "This number requires a uint256, that is not yet supported");
    }
    std::size_t trim_size = 32 - (hex.size() - firstNonZero);
    std::string paddedHex(trim_size, '0');
    paddedHex += hex.substr(firstNonZero);
    // Split the padded hex string into upper and lower parts
    std::string upperHex = paddedHex.substr(0, 16);
    std::string lowerHex = paddedHex.substr(16, 16);

    // Convert the hex parts to uint64_t
    uint64_t upper = HexToUBigInt(upperHex);
    uint64_t lower = HexToUBigInt(lowerHex);

    return {upper, lower};
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
