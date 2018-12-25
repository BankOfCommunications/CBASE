/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License version 2 as
 *   published by the Free Software Foundation.
 *
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *
 */
#include <algorithm>
#include <tbsys.h>

namespace oceanbase
{
  namespace common
  {



    // --------------------------------------------------------
    // class ObVector<T, Allocator> implements
    // --------------------------------------------------------
    template <typename T, typename Allocator>
      ObVector<T, Allocator>::ObVector(Allocator * alloc)
      : mem_begin_(NULL), mem_end_(NULL), mem_end_of_storage_(NULL)
      {
        if(NULL == alloc)
        {
          pallocator_ = &default_allocator_;
        }
        else
        {
          pallocator_ = alloc;
        }
		pallocator_->set_mod_id(ObModIds::VECTOR);
      }

    template <typename T, typename Allocator>
      ObVector<T, Allocator>::ObVector(int32_t size, Allocator * alloc)
      : mem_begin_(NULL), mem_end_(NULL), mem_end_of_storage_(NULL)
      {
        if(NULL == alloc)
        {
          pallocator_ = &default_allocator_;
        }
        else
        {
          pallocator_ = alloc;
        }
        expand(size);
      }

    template <typename T, typename Allocator>
      ObVector<T, Allocator>::~ObVector()
      {
        destroy();
      }

    template <typename T, typename Allocator>
      void ObVector<T, Allocator>::destroy()
      {
        if (mem_begin_)
        {
          pallocator_->free(mem_begin_);
          mem_begin_ = NULL;
          mem_end_ = NULL;
          mem_end_of_storage_ = NULL;
        }
      }

    template <typename T, typename Allocator>
      int ObVector<T, Allocator>::push_back(const_value_type value)
      {
        return insert(end(), value);
      }

    template <typename T, typename Allocator>
      void ObVector<T, Allocator>::clear()
      {
        pallocator_->free(mem_begin_);
        if(pallocator_ == &default_allocator_)
        {
          pallocator_->free();
        }
        mem_end_ = mem_begin_ = mem_end_of_storage_ = NULL;
      }

    /**
     * expand %size bytes memory when buffer not enough.
     * copy origin memory content to new buffer
     * @return
     * <  0  allocate new memory failed
     * == 0  no need expand
     * == %size  expand succeed.
     */
    template <typename T, typename Allocator>
      int32_t ObVector<T, Allocator>::expand(int32_t size)
      {
        int32_t old_size = ObVector<T, Allocator>::size();
        int32_t ret = -1;
        if (old_size < size)
        {

          iterator new_mem = alloc_array(size);
          if (new_mem)
          {
            if (old_size)
            {
              copy(new_mem, mem_begin_, mem_end_);
            }

            // deallocate old memeory
            destroy();

            mem_begin_ = new_mem;
            mem_end_ = mem_begin_ + old_size;
            mem_end_of_storage_ = mem_begin_ + size;
            ret = size;
          }
        }
        else
        {
          ret = 0;
        }
        return ret;
      }

    template <typename T, typename Allocator>
      typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::alloc_array(const int32_t size)
      {
        iterator ptr = reinterpret_cast<iterator>
          (pallocator_->alloc(size * sizeof(value_type)));
        return ptr;
      }

    template <typename T, typename Allocator>
      typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::fill(iterator ptr, const_value_type value)
      {
        if (NULL != ptr)
        {
          //*ptr = value;
          memcpy(ptr, &value, sizeof(value_type));
        }
        return ptr;
      }

    /*
     * [dest, x] && [begin, end] can be overlap
     * @param [in] dest: move dest memory
     * @param [in] begin: source memory start pointer
     * @param [in] end: source memory end pointer
     * @return
     */
    template <typename T, typename Allocator>
      typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::move(iterator dest,
          const_iterator begin, const_iterator end)
      {
        assert(dest);
        assert(end >= begin);
        int32_t n = static_cast<int32_t>(end - begin);
        if (n > 0)
          ::memmove(dest, begin, n * sizeof(value_type));
        return dest + n;
      }

    /*
     * [dest, x] && [begin, end] cannot be overlap
     */
    template <typename T, typename Allocator>
      typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::copy(iterator dest,
          const_iterator begin, const_iterator end)
      {
        assert(dest);
        assert(end >= begin);
        int32_t n = static_cast<int32_t>(end - begin);
        if (n > 0)
          ::memcpy(dest, begin, n * sizeof(value_type));
        return dest + n;
      }

    template <typename T, typename Allocator>
      int ObVector<T, Allocator>::remove(iterator pos)
      {
        int ret = OB_SUCCESS;
        if (pos < mem_begin_ || pos >= mem_end_)
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        else
        {
          iterator new_end_pos = move(pos, pos + 1, mem_end_);
          assert(mem_end_ == new_end_pos + 1);
          mem_end_ = new_end_pos;
        }
        return ret;
      }

    template <typename T, typename Allocator>
      int ObVector<T, Allocator>::remove(iterator start_pos, iterator end_pos)
      {
        int ret = OB_SUCCESS;
        if (start_pos < mem_begin_ || start_pos >= mem_end_
          || end_pos < mem_begin_ || end_pos > mem_end_)
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        else if (end_pos - start_pos > 0)
        {
          iterator new_end_pos = move(start_pos, end_pos, mem_end_);
          assert(mem_end_ == new_end_pos + (end_pos - start_pos));
          mem_end_ = new_end_pos;
        }
        return ret;
      }

    template <typename T, typename Allocator>
      int ObVector<T, Allocator>::remove(const int32_t index)
      {
        int ret = OB_SUCCESS;
        if (index >= 0 && index < size())
        {
          ret = remove(mem_begin_ + index);
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        return ret;
      }

    template <typename T, typename Allocator>
      int ObVector<T, Allocator>::remove_if(const_value_type value)
      {
        int ret = OB_SUCCESS;
        iterator pos = mem_begin_;
        while (pos != mem_end_)
        {
          if (*pos == value) break;
          ++pos;
        }

        if (pos >= mem_end_) ret = OB_ENTRY_NOT_EXIST;
        else ret = remove(pos);

        return ret;
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Predicate>
      int ObVector<T, Allocator>::remove_if(const ValueType& value, Predicate predicate)
      {
        int ret = OB_SUCCESS;
        iterator pos = mem_begin_;
        while (pos != mem_end_)
        {
          if (predicate(*pos, value)) break;
          ++pos;
        }

        if (pos >= mem_end_) ret = OB_ENTRY_NOT_EXIST;
        else ret = remove(pos);

        return ret;
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Predicate>
      int ObVector<T, Allocator>::remove_if(const ValueType& value, Predicate predicate, value_type & removed_value)
      {
        int ret = OB_SUCCESS;
        iterator pos = mem_begin_;
        while (pos != mem_end_)
        {
          if (predicate(*pos, value)) break;
          ++pos;
        }

        if (pos >= mem_end_)
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          removed_value = *pos;
          ret = remove(pos);
        }

        return ret;
      }

    template <typename T, typename Allocator>
      int ObVector<T, Allocator>::insert(iterator pos, const_value_type value)
      {
        if (remain() >= 1)
        {
          if (pos == mem_end_)
          {
            fill(pos, value);
            mem_end_ += 1;
          }
          else
          {
            move(pos + 1, pos, mem_end_);
            fill(pos, value);
            mem_end_ += 1;
          }
        }
        else
        {
          // need expand
          int32_t old_size = size();
          int32_t new_size = (old_size + 1) << 1;
          iterator new_mem = alloc_array(new_size);
          if (!new_mem) return OB_ERROR;
          iterator new_end = new_mem + old_size + 1;
          if (pos == mem_end_)
          {
            copy(new_mem, mem_begin_, mem_end_);
            fill(new_mem + old_size, value);
          }
          else
          {
            // copy head part;
            iterator pivot = copy(new_mem, mem_begin_, pos);
            // copy tail part;
            copy(pivot + 1, pos, mem_end_);
            fill(pivot, value);
          }

          // free old memory and reset pointers..
          destroy();
          mem_begin_ = new_mem;
          mem_end_ = new_end;
          mem_end_of_storage_ = new_mem + new_size;
        }
        return OB_SUCCESS;
      }

    // ObSortedVector
    template <typename T, typename Allocator>
      template <typename Compare>
      int ObSortedVector<T, Allocator>::insert(const_value_type value,
          iterator & insert_pos, Compare compare)
      {
        int ret = OB_SUCCESS;
        if (NULL == value) ret = OB_ERROR;
        insert_pos = vector_.end();
        if (OB_SUCCESS == ret)
        {
          iterator find_pos = std::lower_bound(vector_.begin(), vector_.end(), value, compare);
          insert_pos = find_pos;

          ret = vector_.insert(insert_pos, value);
        }
        return ret;
      }

    template <typename T, typename Allocator>
      template <typename Compare, typename Unique>
      int ObSortedVector<T, Allocator>::insert_unique(const_value_type value,
          iterator & insert_pos, Compare compare, Unique unique)
      {
        int ret = OB_SUCCESS;
        if (NULL == value) ret = OB_ERROR;
        iterator begin_iterator = begin();
        iterator end_iterator = end();
        insert_pos = end_iterator;
        if (OB_SUCCESS == ret)
        {
          iterator find_pos = std::lower_bound(begin_iterator,
              end_iterator, value, compare);
          insert_pos = find_pos;
          iterator compare_pos = find_pos;
          iterator prev_pos = end_iterator;

          if (find_pos == end_iterator && size() > 0)
            compare_pos = last();
          if (find_pos > begin_iterator && find_pos < end_iterator)
            prev_pos = find_pos - 1;

          if (compare_pos >= begin_iterator && compare_pos < end_iterator)
          {
            if (unique(*compare_pos, value))
            {
              insert_pos = compare_pos;
              ret = OB_CONFLICT_VALUE;
            }
          }

          if (OB_SUCCESS == ret && prev_pos >= begin_iterator && prev_pos < end_iterator)
          {
            if (unique(*prev_pos, value))
            {
              insert_pos = prev_pos;
              ret = OB_CONFLICT_VALUE;
            }
          }


          if (OB_SUCCESS == ret)
            ret = vector_.insert(insert_pos, value);
        }
        return ret;
      }

    template <typename T, typename Allocator>
      template <typename Compare>
      int ObSortedVector<T, Allocator>::find(const_value_type value,
          iterator& pos, Compare compare) const
      {
        int ret = OB_ENTRY_NOT_EXIST;
        pos = std::lower_bound(begin(), end(), value, compare);
        if (pos != end())
        {
          if (!compare(*pos, value) && !compare(value, *pos))
            ret = OB_SUCCESS;
          else
            pos = end();
        }
        return ret;
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Compare, typename Equal>
      int ObSortedVector<T, Allocator>::find(const ValueType &value,
          iterator& pos, Compare compare, Equal equal) const
      {
        int ret = OB_ENTRY_NOT_EXIST;
        pos = std::lower_bound(begin(), end(), value, compare);
        if (pos != end())
        {
          if (equal(*pos, value))
            ret = OB_SUCCESS;
          else
            pos = end();
        }
        return ret;
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Compare>
      typename ObSortedVector<T, Allocator>::iterator
      ObSortedVector<T, Allocator>::lower_bound(const ValueType &value, Compare compare) const
      {
        return std::lower_bound(begin(), end(), value, compare);
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Compare>
      typename ObSortedVector<T, Allocator>::iterator
      ObSortedVector<T, Allocator>::upper_bound(const ValueType &value, Compare compare) const
      {
        return std::upper_bound(begin(), end(), value, compare);
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Compare, typename Equal>
      int ObSortedVector<T, Allocator>::remove_if(const ValueType& value,
          Compare comapre, Equal equal)
      {
        iterator pos = end();
        int ret = find(value, pos, comapre, equal);
        if (OB_SUCCESS == ret)
        {
          ret = vector_.remove(pos);
        }
        return ret;
      }

    template <typename T, typename Allocator>
      template <typename ValueType, typename Compare, typename Equal>
      int ObSortedVector<T, Allocator>::remove_if(const ValueType& value,
          Compare comapre, Equal equal, value_type &removed_value)
      {
        iterator pos = end();
        int ret = find(value, pos, comapre, equal);
        if (OB_SUCCESS == ret)
        {
          removed_value = *pos;
          ret = vector_.remove(pos);
        }
        return ret;
      }

    template <typename T, typename Allocator>
      int ObSortedVector<T, Allocator>::remove(iterator start_pos, iterator end_pos)
      {
        int ret = OB_SUCCESS;
        if (end_pos - start_pos > 0)
        {
          ret = vector_.remove(start_pos, end_pos);
        }
        return ret;
      }

  } // end namespace common
} // end namespace oceanbase
