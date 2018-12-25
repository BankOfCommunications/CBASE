#ifndef OCEANBASE_COMMON_BITMAPSET_H_
#define OCEANBASE_COMMON_BITMAPSET_H_

#include <stdint.h>
#include "common/utility.h"

namespace oceanbase
{
  namespace common
  {
    template <int32_t N = 256>
    class ObBitSet
    {
    public:
      typedef uint32_t BitSetWord;

      ObBitSet();
      virtual ~ObBitSet() {}

      bool add_member(int32_t index);
      bool del_member(int32_t index);
      bool has_member(int32_t index) const;
      bool is_empty() const;
      bool is_subset(const ObBitSet& other) const;
      bool is_superset(const ObBitSet& other) const;
      bool overlap(const ObBitSet& other) const;
      void add_members(const ObBitSet& other);
      void clear();
      int32_t num_members() const;
      int compare_bit(const ObBitSet & other, int32_t bit) const;
      BitSetWord get_bitset_word(int32_t index) const;

      ObBitSet(const ObBitSet &other);
      ObBitSet& operator=(const ObBitSet &other);

      bool operator==(const ObBitSet &other) const;
      bool equal(const ObBitSet &other) const;

    private:
      static const int32_t PER_BITMAPWORD_BITS = 32;
      static const int32_t MAX_BITMAPWORD = (N - 1) / PER_BITMAPWORD_BITS + 1;

      BitSetWord bitset_words_[MAX_BITMAPWORD];
    };

    template <int32_t N>
    ObBitSet<N>::ObBitSet()
    {
      for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
        bitset_words_[i] = 0;
    }

    template <int32_t N>
    int ObBitSet<N>::compare_bit(const ObBitSet & other, int32_t bit) const
    {
      int ret = 0;
      if (has_member(bit) == other.has_member(bit))
      {
        // do nothing
      }
      else if (!has_member(bit))
      {
        ret = -1;
      }
      else
      {
        ret = 1;
      }
      return ret;
    }

    template <int32_t N>
    bool ObBitSet<N>::add_member(int32_t index)
    {
      if (index < 0)
      {
        TBSYS_LOG(ERROR, "negative bitmapset member not allowed");
        return false;
      }
      if (index >= MAX_BITMAPWORD * PER_BITMAPWORD_BITS)
      {
        TBSYS_LOG(ERROR, "bitmap index exceeds the scope");
        return false;
      }
      bitset_words_[index / PER_BITMAPWORD_BITS] |= ((BitSetWord) 1 << (index % PER_BITMAPWORD_BITS));
      return true;
    }

    template <int32_t N>
    bool ObBitSet<N>::del_member(int32_t index)
    {
      if (index < 0)
      {
        TBSYS_LOG(ERROR, "negative bitmapset member not allowed");
        return false;
      }
      if (index >= MAX_BITMAPWORD * PER_BITMAPWORD_BITS)
        return true;
      bitset_words_[index / PER_BITMAPWORD_BITS] &= ~((BitSetWord) 1 << (index % PER_BITMAPWORD_BITS));
      return true;
    }

    template <int32_t N>
    typename ObBitSet<N>::BitSetWord ObBitSet<N>::get_bitset_word(int32_t index) const
    {
      if (index < 0 || index >= MAX_BITMAPWORD)
      {
        TBSYS_LOG(ERROR, "bitmap word index exceeds the scope");
        return 0;
      }
      return bitset_words_[index];
    }

    template <int32_t N>
    void ObBitSet<N>::add_members(const ObBitSet& other)
    {
      for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
      {
        bitset_words_[i] |= other.get_bitset_word(i);
      }
    }

    template <int32_t N>
    bool ObBitSet<N>::has_member(int32_t index) const
    {
      if (index < 0)
      {
        TBSYS_LOG(ERROR, "negative bitmapset member not allowed");
        return false;
      }
      if (index >= MAX_BITMAPWORD * PER_BITMAPWORD_BITS)
      {
        TBSYS_LOG(ERROR, "bitmap index exceeds the scope");
        return false;
      }
      return ((bitset_words_[index / PER_BITMAPWORD_BITS]
          & ((BitSetWord) 1 << (index % PER_BITMAPWORD_BITS))) != 0);
    }

    template <int32_t N>
    bool ObBitSet<N>::is_empty() const
    {
      for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
      {
        if (bitset_words_[i] != 0)
          return false;
      }
      return true;
    }

    template <int32_t N>
    bool ObBitSet<N>::is_superset(const ObBitSet& other) const
    {
      bool ret = true;
      if (other.is_empty())
        return true;
      else if (is_empty())
        return false;
      else
      {
        for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
        {
          if ((other.get_bitset_word(i)) & ~(bitset_words_[i]))
            return false;
        }
      }
      return ret;
    }

    template <int32_t N>
    bool ObBitSet<N>::is_subset(const ObBitSet& other) const
    {
      bool ret = true;
      if (is_empty())
        ret = true;
      else if (other.is_empty())
        ret = false;
      else
      {
        for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
        {
          if (bitset_words_[i] & ~(other.get_bitset_word(i)))
            return false;
        }
      }
      return ret;
    }

    template <int32_t N>
    bool ObBitSet<N>::overlap(const ObBitSet& other) const
    {
      bool ret = false;
      for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
      {
        if (bitset_words_[i] & other.get_bitset_word(i))
        {
          ret = true;
          break;
        }
      }
      return ret;
    }

    template <int32_t N>
    void ObBitSet<N>::clear()
    {
      memset(bitset_words_, 0, sizeof(bitset_words_));
    }

    template <int32_t N>
    int32_t ObBitSet<N>::num_members() const
    {
      int32_t num = 0;
      BitSetWord word;

      for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
      {
        word = get_bitset_word(i);
        if (!word)
          break;
        word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
        word = (word & UINT32_C(0x33333333)) + ((word >> 1) & UINT32_C(0x33333333));
        word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 1) & UINT32_C(0x0f0f0f0f));
        word = (word & UINT32_C(0x00ff00ff)) + ((word >> 1) & UINT32_C(0x00ff00ff));
        word = (word & UINT32_C(0x0000ffff)) + ((word >> 1) & UINT32_C(0x0000ffff));
        num += (int32_t)word;
      }
      return num;
    }

    template <int32_t N>
    ObBitSet<N>::ObBitSet(const ObBitSet &other)
    {
      *this = other;
    }

    template <int32_t N>
    ObBitSet<N>& ObBitSet<N>::operator=(const ObBitSet &other)
    {
      for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
      {
        this->bitset_words_[i] = other.get_bitset_word(i);
      }
      return *this;
    }

    template <int32_t N>
    bool ObBitSet<N>::operator==(const ObBitSet &other) const
    {
      bool ret = true;

      if (this->num_members() != other.num_members())
      {
        ret = false;
      }
      else
      {
        for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
        {
          if (this->bitset_words_[i] != other.get_bitset_word(i))
          {
            ret = false;
            break;
          }
        }
      }

      return ret;
    }

    template <int32_t N>
    bool ObBitSet<N>::equal(const ObBitSet &other) const
    {
      return *this == other;
    }
  }
}

#endif //OCEANBASE_COMMON_BITMAPSET_H_


