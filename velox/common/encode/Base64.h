/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <array>
#include <exception>
#include <map>
#include <string>

#include <folly/Range.h>
#include <folly/io/IOBuf.h>

#include "velox/common/base/GTestMacros.h"

namespace facebook::velox::encoding {

class Base64 {
 public:
  static const size_t kCharsetSize = 64;
  static const size_t kReverseIndexSize = 256;

  /// Character set used for encoding purposes.
  /// Contains specific characters that form the encoding scheme.
  using Charset = std::array<char, kCharsetSize>;

  /// Reverse lookup table for decoding purposes.
  /// Maps each possible encoded character to its corresponding numeric value
  /// within the encoding base.
  using ReverseIndex = std::array<uint8_t, kReverseIndexSize>;

  /// Padding character used in encoding.
  static const char kPadding = '=';

  /// Encodes the specified number of characters from the 'data'.
  static std::string encode(const char* data, size_t len);

  /// Encodes the specified text.
  static std::string encode(folly::StringPiece text);

  /// Encodes the specified IOBuf data.
  static std::string encode(const folly::IOBuf* text);

  /// Returns encoded size for the input of the specified size.
  static size_t calculateEncodedSize(size_t size, bool withPadding = true);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateEncodedSize().
  static void encode(const char* data, size_t size, char* output);

  /// Decodes the specified encoded text.
  static std::string decode(folly::StringPiece encoded);

  /// Returns the actual size of the decoded data. Will also remove the padding
  /// length from the input data 'size'.
  static size_t calculateDecodedSize(const char* data, size_t& size);

  /// Decodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateDecodedSize().
  static void decode(const char* data, size_t size, char* output);

  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output' using URL encoding. The output must have enough
  /// space as returned by the calculateEncodedSize().
  static void encodeUrl(const char* data, size_t size, char* output);

  /// Encodes the specified number of characters from the 'data' using URL
  /// encoding.
  static std::string encodeUrl(const char* data, size_t len);

  /// Encodes the specified IOBuf data using URL encoding.
  static std::string encodeUrl(const folly::IOBuf* data);

  /// Encodes the specified text using URL encoding.
  static std::string encodeUrl(folly::StringPiece text);

  /// Decodes the specified URL encoded payload and writes the result to the
  /// 'output'.
  static void decodeUrl(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified URL encoded text.
  static std::string decodeUrl(folly::StringPiece text);

  /// Decodes the specified number of characters from the 'src' and writes the
  /// result to the 'dst'.
  static size_t
  decode(const char* src, size_t src_len, char* dst, size_t dst_len);

  /// Decodes the specified number of characters from the 'src' using URL
  /// encoding and writes the result to the 'dst'.
  static void
  decodeUrl(const char* src, size_t src_len, char* dst, size_t dst_len);

 private:
  /// Checks if there is padding in encoded data.
  static inline bool isPadded(const char* data, size_t len) {
    return (len > 0 && data[len - 1] == kPadding);
  }

  /// Counts the number of padding characters in encoded data.
  static inline size_t numPadding(const char* src, size_t len) {
    size_t numPadding{0};
    while (len > 0 && src[len - 1] == kPadding) {
      numPadding++;
      len--;
    }
    return numPadding;
  }

  /// Performs a reverse lookup in the reverse index to retrieve the original
  /// index of a character in the base.
  static uint8_t base64ReverseLookup(char p, const ReverseIndex& reverseIndex);

  /// Encodes the specified data using the provided charset.
  template <class T>
  static std::string
  encodeImpl(const T& data, const Charset& charset, bool include_pad);

  /// Encodes the specified data using the provided charset.
  template <class T>
  static void encodeImpl(
      const T& data,
      const Charset& charset,
      bool include_pad,
      char* out);

  /// Decodes the specified data using the provided reverse lookup table.
  static size_t decodeImpl(
      const char* src,
      size_t src_len,
      char* dst,
      size_t dst_len,
      const ReverseIndex& table);

  VELOX_FRIEND_TEST(Base64Test, checksPadding);
  VELOX_FRIEND_TEST(Base64Test, countsPaddingCorrectly);
};

} // namespace facebook::velox::encoding
