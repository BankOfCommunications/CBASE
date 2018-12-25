#ifndef OCEANBASE_COMMON_MURMURHASH_H_
#define OCEANBASE_COMMON_MURMURHASH_H_
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
namespace oceanbase
{
  namespace common
  {

    /**
     * The MurmurHash 2 from Austin Appleby, faster and better mixed (but weaker
     * crypto-wise with one pair of obvious differential) than both Lookup3 and
     * SuperFastHash. Not-endian neutral for speed.
     */

    uint32_t murmurhash2(const void *data, int32_t len, uint32_t hash);

    /**
     * MurmurHash2, 64-bit versions, by Austin Appleby
     * The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
     * and endian-ness issues if used across multiple platforms.
     * 64-bit hash for 64-bit platforms
     */
    uint64_t murmurhash64A(const void* key, int32_t len, uint64_t seed);

    uint32_t fnv_hash2(const void *data, int32_t len, uint32_t hash);

    struct MurmurHash2
    {
      uint32_t operator()(const std::string& s) const
      {
        return murmurhash2(s.c_str(), static_cast<int32_t>(s.length()), 0);
      }

      uint32_t operator()(const void *start, int32_t len) const
      {
        return murmurhash2(start, len, 0);
      }

      uint32_t operator()(const void *start, int32_t len, uint32_t seed) const
      {
        return murmurhash2(start, len, seed);
      }

      uint32_t operator()(const char *s) const
      {
        return murmurhash2(s, static_cast<int32_t>(strlen(s)), 0);
      }
    };

    struct MurmurHash64A
    {
      uint64_t operator()(const std::string& s) const
      {
        return murmurhash64A(s.c_str(), static_cast<int32_t>(s.length()), 0);
      }

      uint64_t operator()(const void* start, int32_t len) const
      {
        return murmurhash64A(start, len, 0);
      }

      uint64_t operator()(const void* start, int32_t len, uint64_t seed) const
      {
        return murmurhash64A(start, len, seed);
      }

      uint64_t operator()(const char* s) const
      {
        return murmurhash64A(s, static_cast<int32_t>(strlen(s)), 0);
      }
    };
  }
}
#endif
