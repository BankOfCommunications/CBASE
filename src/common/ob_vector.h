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
#ifndef OCEANBASE_COMMON_OB_VECTOR_H_
#define OCEANBASE_COMMON_OB_VECTOR_H_

#include "common/page_arena.h"

namespace oceanbase 
{ 
  namespace common 
  {
    template <typename T> class ob_vector_traits;

    template <typename T> 
      struct ob_vector_traits<T*>
      {
        public:
          typedef T pointee_type;
          typedef T* value_type;
          typedef const T* const_value_type;
          typedef value_type* iterator;
          typedef const value_type* const_iterator;
          typedef int32_t difference_type;
      };

    template <typename T> 
      struct ob_vector_traits<const T*>
      {
        typedef T pointee_type;
        typedef const T* value_type;
        typedef const T* const const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<int64_t>
      {
        typedef int64_t& pointee_type;
        typedef int64_t value_type;
        typedef const int64_t const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };
    
    template <typename T, typename Allocator = PageArena<T> >
      class ObVector
      {
        public:
          typedef typename ob_vector_traits<T>::pointee_type pointee_type;
          typedef typename ob_vector_traits<T>::value_type value_type;
          typedef typename ob_vector_traits<T>::const_value_type const_value_type;
          typedef typename ob_vector_traits<T>::iterator iterator;
          typedef typename ob_vector_traits<T>::const_iterator const_iterator;
          typedef typename ob_vector_traits<T>::difference_type difference_type;
        public:
          ObVector(Allocator * alloc = NULL);
          explicit ObVector(int32_t size, Allocator * alloc = NULL);
          virtual ~ObVector();
        public:
          int32_t size() const { return static_cast<int32_t>(mem_end_ - mem_begin_); }
          int32_t capacity() const { return static_cast<int32_t>(mem_end_of_storage_ - mem_begin_); }
          int32_t remain() const { return static_cast<int32_t>(mem_end_of_storage_ - mem_end_); }
        public:
          iterator begin() const { return mem_begin_; }
          iterator end() const { return mem_end_; }
          iterator last() const { return mem_end_ - 1; }
          Allocator & get_allocator() const { return *pallocator_; }
          void set_allocator(Allocator & allc){ pallocator_ = &allc; }


          inline bool at(const int32_t index, value_type& v) const
          {
            bool ret = false;
            if (index >= 0 && index < size()) 
            {
              v = *(mem_begin_ + index);
              ret = true;
            }
            return ret;
          }

          inline value_type & at(const int32_t index) const 
          { 
            assert(index >= 0 && index < size()); 
            return *(mem_begin_ + index); 
          }
          inline value_type & operator[](const int32_t index) const 
          {
            return at(index);
          }

        public:
          int push_back(const_value_type value);
          int insert(iterator pos, const_value_type value);
          int remove(iterator pos);
          int remove(const int32_t index);
          int remove(iterator start_pos, iterator end_pos);
          int remove_if(const_value_type value);
          template <typename ValueType, typename Predicate>
            int remove_if(const ValueType& value, Predicate predicate);
          template <typename ValueType, typename Predicate>
            int remove_if(const ValueType& value, Predicate predicate, value_type & removed_value);
          int reserve(int32_t size) { return expand(size); }
          void clear();
          inline void reset() { mem_end_ = mem_begin_; }
        private:
          void destroy();
          int  expand(int32_t size);
        private:
          iterator  alloc_array(const int32_t size);

          static iterator fill(iterator ptr, const_value_type value);
          static iterator copy(iterator dest, const_iterator begin, const_iterator end);
          static iterator move(iterator dest, const_iterator begin, const_iterator end);
        protected:
          iterator mem_begin_;
          iterator mem_end_;
          iterator mem_end_of_storage_;
          mutable Allocator default_allocator_;
          Allocator *pallocator_;
      };

    template <typename T, typename Allocator = PageArena<T> > 
      class ObSortedVector 
      {
        public:
          typedef Allocator allocator_type;
          typedef ObVector<T, Allocator> vector_type;
          typedef typename vector_type::iterator iterator;
          typedef typename vector_type::const_iterator const_iterator;
          typedef typename vector_type::value_type value_type;
          typedef typename vector_type::const_value_type const_value_type;
          typedef typename vector_type::difference_type difference_type;

        public:
          ObSortedVector() {}
          explicit ObSortedVector(int32_t size) : vector_(size) {}
          virtual ~ObSortedVector() {}

        public:
          inline int32_t size() const { return vector_.size(); }
          inline int32_t capacity() const { return vector_.capacity(); }
          inline int32_t remain() const { return vector_.remain(); }
          inline int reserve(int32_t size) { return vector_.reserve(size); }
          inline int remove(iterator pos) { return vector_.remove(pos); }
          inline void clear() { return vector_.clear(); }
          inline void reset() { return vector_.reset(); };

        public:
          inline iterator begin() const { return vector_.begin(); }
          inline iterator end() const { return vector_.end(); }
          inline iterator last() const { return vector_.last(); }
          inline Allocator & get_allocator() const { return vector_.get_allocator(); }

        public:
          inline bool at(const int32_t index, value_type& v) const
          {
            return vector_.at(index, v);
          }

          inline value_type & at(const int32_t index) const 
          { 
            return vector_.at(index);
          }

          inline value_type & operator[](const int32_t index) const 
          {
            return at(index);
          }

        public:
          template <typename Compare>
            int insert(const_value_type value, iterator& insert_pos, Compare compare);
          template <typename Compare, typename Unique>
            int insert_unique(const_value_type value, 
                iterator& insert_pos, Compare compare, Unique unique);
          template <typename Compare>
            int find(const_value_type value, iterator &pos, Compare compare) const;

          template <typename ValueType, typename Compare>
            iterator lower_bound(const ValueType &value, Compare compare) const;
          template <typename ValueType, typename Compare>
            iterator upper_bound(const ValueType &value, Compare compare) const;

          template <typename ValueType, typename Compare, typename Equal>
            int find(const ValueType &value, iterator &pos, Compare compare, Equal equal) const;
          template <typename ValueType, typename Compare, typename Equal>
            int remove_if(const ValueType& value, Compare comapre, Equal equal);
          template <typename ValueType, typename Compare, typename Equal>
            int remove_if(const ValueType& value, Compare comapre, Equal equal, value_type & removed_value);

          int remove(iterator start_pos, iterator end_pos);

        private:
          ObVector<T, Allocator> vector_;

      };

  } // end namespace common
} // end namespace oceanbase

#include "ob_vector.ipp"


#endif //OCEANBASE_ROOTSERVER_ROOTMETA_H_
