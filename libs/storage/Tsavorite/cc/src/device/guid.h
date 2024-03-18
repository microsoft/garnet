// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#pragma warning(disable : 4996)

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

#ifdef _WIN32
#define NOMINMAX
#define _WINSOCKAPI_
#include <Windows.h>
#else
#include <uuid/uuid.h>
#endif

namespace FASTER {
namespace core {

/// Wrapper for GUIDs, for Windows and Linux.
class Guid {
 public:
#ifdef _WIN32
  Guid() {
    guid_.Data1 = 0;
    guid_.Data2 = 0;
    guid_.Data3 = 0;
    std::memset(guid_.Data4, 0, 8);
  }
#else
  Guid() {
    uuid_clear(uuid_);
  }
#endif

 private:
#ifdef _WIN32
  Guid(const GUID& guid)
    : guid_{ guid } {
  }
#else
  Guid(const uuid_t uuid) {
    uuid_copy(uuid_, uuid);
  }
#endif

 public:
  static Guid Create() {
#ifdef _WIN32
    GUID guid;
    HRESULT result = ::CoCreateGuid(&guid);
    assert(result == S_OK);
    return guid;
#else
    uuid_t uuid;
    uuid_generate(uuid);
    return uuid;
#endif
  }

  static Guid Parse(const std::string str) {
#ifdef _WIN32
    GUID guid;
    auto result = ::UuidFromString(reinterpret_cast<uint8_t*>(const_cast<char*>(str.c_str())),
                                   &guid);
    assert(result == RPC_S_OK);
    return guid;
#else
    uuid_t uuid;
    int result = uuid_parse(const_cast<char*>(str.c_str()), uuid);
    assert(result == 0);
    return uuid;
#endif
  }

  std::string ToString() const {
    char buffer[37];
#ifdef _WIN32
    size_t offset = sprintf(buffer, "%.8lX-%.4hX-%.4hX-", guid_.Data1, guid_.Data2, guid_.Data3);
    for(size_t idx = 0; idx < 2; ++idx) {
      offset += sprintf(buffer + offset, "%.2hhX", guid_.Data4[idx]);
    }
    offset += sprintf(buffer + offset, "-");
    for(size_t idx = 2; idx < sizeof(guid_.Data4); ++idx) {
      offset += sprintf(buffer + offset, "%.2hhX", guid_.Data4[idx]);
    }
    buffer[36] = '\0';
#else
    uuid_unparse(uuid_, buffer);
#endif
    return std::string { buffer };
  }

  bool operator==(const Guid& other) const {
#ifdef _WIN32
    return guid_.Data1 == other.guid_.Data1 &&
           guid_.Data2 == other.guid_.Data2 &&
           guid_.Data3 == other.guid_.Data3 &&
           std::memcmp(guid_.Data4, other.guid_.Data4, 8) == 0;
#else
    return uuid_compare(uuid_, other.uuid_) == 0;
#endif
  }

  uint32_t GetHashCode() const {
#ifdef _WIN32
    // From C#, .NET Reference Framework.
    return guid_.Data1 ^ ((static_cast<uint32_t>(guid_.Data2) << 16) |
                          static_cast<uint32_t>(guid_.Data3)) ^
           ((static_cast<uint32_t>(guid_.Data4[2]) << 24) | guid_.Data4[7]);
#else
    uint32_t Data1;
    uint16_t Data2;
    uint16_t Data3;
    std::memcpy(&Data1, uuid_, sizeof(Data1));
    std::memcpy(&Data2, uuid_ + 4, sizeof(Data2));
    std::memcpy(&Data3, uuid_ + 6, sizeof(Data3));
    // From C#, .NET Reference Framework.
    return Data1 ^ ((static_cast<uint32_t>(Data2) << 16) |
                    static_cast<uint32_t>(Data3)) ^
           ((static_cast<uint32_t>(uuid_[10]) << 24) | uuid_[15]);
#endif
  }

 private:
#ifdef _WIN32
  GUID guid_;
#else
  uuid_t uuid_;
#endif
};

}
} // namespace FASTER::core

/// Implement std::hash<> for GUIDs.
namespace std {
template<>
struct hash<FASTER::core::Guid> {
  size_t operator()(const FASTER::core::Guid& val) const {
    return val.GetHashCode();
  }
};
}
