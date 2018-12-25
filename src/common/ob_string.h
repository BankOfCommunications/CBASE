#ifndef OCEANBASE_COMMON_OB_STRING_H_
#define OCEANBASE_COMMON_OB_STRING_H_

#include "serialization.h"
#include "ob_define.h"
#include "data_buffer.h"
#include "murmur_hash.h"
#include <algorithm>
#include <iostream>
namespace oceanbase
{
  namespace common
  {
    /**
     * ObString do not own the buffer's memory
     * @ptr_ : buffer pointer, allocated by user.
     * @buffer_size_ : buffer's capacity
     * @data_length_ : actual data length of %ptr_
     */
    class ObString
    {
      public:
        typedef int32_t obstr_size_t;
      public:
        ObString()
          : buffer_size_(0), data_length_(0), ptr_(NULL)
        {
        }

        INLINE_NEED_SERIALIZE_AND_DESERIALIZE;

        /*
        ObString(obstr_size_t size, char* ptr)
          : size_(size), length_(0), ptr_(ptr)
        {
          assert(length_ <= size_);
        }
        */

        /*
         * attach the buff start from ptr, capacity is size, data length is length
         * 用一块以ptr标识的, 大小为 size, 其中已经存在了 length 长度数据 的buf 初始化
         */

        ObString(const obstr_size_t size, const obstr_size_t length, char* ptr)
          : buffer_size_(size), data_length_(length), ptr_(ptr)
        {
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }
        }

        ObString(const obstr_size_t size, const obstr_size_t length, const char* ptr)
          : buffer_size_(size), data_length_(length), ptr_(const_cast<char*>(ptr))
        {
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }
        }

        /*
         * attache myself to buf, and then copy rv's data to myself.
         * 把rv中的obstring拷贝到buf中, 并且把自己和该buf建立联系
         *
         */

        inline int clone(const ObString& rv, ObDataBuffer& buf)
        {
          int ret = OB_ERROR;
          assign_buffer(buf.get_data(), static_cast<obstr_size_t>(buf.get_remain()));
          obstr_size_t writed_length = write(rv.ptr(), rv.length());
          if (writed_length == rv.length())
          {
            ret = OB_SUCCESS;
            buf.get_position() += writed_length;
          }
          return ret;
        }

        inline int clone(const ObString& src)
        {
          int ret = OB_BUF_NOT_ENOUGH;
          obstr_size_t writed_length = write(src.ptr(), src.length());
          if (writed_length == src.length())
          {
            ret = OB_SUCCESS;
          }
          return ret;
        }

        inline const char* find(const char c)
        {
          const char* ret = NULL;
          if (NULL != ptr_)
          {
            for(obstr_size_t i=0; i < data_length_; ++i)
            {
              if (ptr_[i] == c)
              {
                ret = &ptr_[i];
                break;
              }
            }
          }
          return ret;
        }

        // not virtual, i'm not a base class.
        ~ObString()
        {
        }
        // reset
        void reset()
        {
          buffer_size_ = 0;
          data_length_ = 0;
          ptr_ = NULL;
        }
        // ObString 's copy constructor && assignment, use default, copy every member.
        // ObString(const ObString & obstr);
        // ObString & operator=(const ObString& obstr);

        /*
         * write a stream to my buffer,
         *
         * 向自己的buf里写入一段字节流
         * 返回值是成功写入的字节数
         *
         */

        inline obstr_size_t write(const char* bytes, const obstr_size_t length)
        {
          obstr_size_t writed = 0;
          if (!bytes || length <= 0 )
          {
            ;
          }
          else
          {
            if (data_length_ + length <= buffer_size_)
            {
              memcpy(ptr_ + data_length_, bytes, length);
              data_length_ += length;
              writed = length;
            }
          }
          return writed;
        }

        /*
         * DO NOT USE THIS ANY MORE
         *
         * 请不要使用这个接口, 后期可能废弃
         */

        inline void assign(char* bytes, const obstr_size_t length)
        {
          ptr_ = bytes;
          buffer_size_ = length;
          data_length_ = length;
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }
        }

        /*
         * attach myself to other's buff, so you can read through me, but not write
         * 把ob_string 关联到一块不属于自己的内存空间上, 该 obstring 可读,不可写
         */

        inline void assign_ptr(char* bytes, const obstr_size_t length)
        {
          ptr_ = bytes;
          buffer_size_ = 0;   //this means I do not hold the buff, just a ptr
          data_length_ = length;
          if (ptr_ == NULL)
          {
            data_length_ = 0;
          }
        }

        /*
         * attach myself to a buffer, whoes capacity is size
         * 把自己与一片buffer 关联起来
         */

        inline void assign_buffer(char* buffer, const obstr_size_t size)
        {
          ptr_ = buffer;
          buffer_size_ = size;  //this means I hold the buffer, so you can do write
          data_length_ = 0;
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }

        }

        /*
         * the remain size of my buffer
         * 当自己hold buffer的时候, 是buffer中还没使用的长度
         * 当仅仅是保存一个指针的时候, 返回0
         */

        inline obstr_size_t remain() const
        {

          return buffer_size_ > 0 ? (buffer_size_ - data_length_): buffer_size_;
        }

        inline obstr_size_t length() const { return data_length_; }
        inline obstr_size_t size() const { return buffer_size_; }
        inline const char* ptr() const { return ptr_; }
        inline char* ptr() { return ptr_; }

        inline int64_t hash() const
        {
          int64_t hash_val = 0;
          if (NULL != ptr_ && data_length_ > 0)
          {
		    //mod zhaoqiong [varchar extend] 20151103:b
            //hash_val = murmurhash2(ptr_, data_length_, 0);
            int32_t actual_len = data_length_;
            while (actual_len > 0)
            {
              if (ptr_[actual_len-1] != ' ')
              {
                break;
              }
              actual_len --;
            }
            hash_val = murmurhash2(ptr_, actual_len, 0);
			//mod:e
          }
          return hash_val;
        };

        inline int32_t compare(const ObString& obstr) const
        {
          int32_t ret = 0;
          if (ptr_ == obstr.ptr_)
          {
            ret = data_length_ - obstr.data_length_;
          }
          else if ( 0 == (ret = ::memcmp(ptr_,obstr.ptr_,std::min(data_length_,obstr.data_length_)) ))
          {
            ret = data_length_ - obstr.data_length_;
			//mod zhaoqiong [varchar extend] 20151103:b
            const char * str = NULL;
            if ( 0 < ret )
            {
              str = ptr_;
            }
            else if ( 0 > ret )
            {
              str = obstr.ptr();
            }
            int32_t pos = std::min(data_length_,obstr.data_length_);
            while ( pos < std::max(data_length_,obstr.data_length_))
            {
              if ( str[pos] != ' ')
              {
                break;
              }
              pos ++;
            }
            if ( pos == std::max(data_length_,obstr.data_length_))
            {
              ret = 0;
            }
			//mod:e
          }
          return ret;
        }

        inline int32_t compare(const char* str) const
        {
          int32_t len = 0;
          if (str != NULL)
          {
            len = static_cast<int32_t>(strlen(str));
          }
          char * p = (char*)str;
          ObString rv(0, len, p);
          return compare(rv);
        }

        inline obstr_size_t shrink()
        {

          obstr_size_t rem  = remain();
          if (buffer_size_ > 0) {
            buffer_size_ = data_length_;
          }
          return rem;
        }
        //add dolphin [database manager]@20150613
        /**
         * do the src1 + src2 operation
         * @param src1 the first source obstring
         * @param src2 the second source obstring
         * @param start from which location in dest do the copy operation
         * @return the total copy size
         * @note  Be careful to the obstring buffer_size_,you manage the buf
         */
        inline bool concat( const ObString& src1, const ObString& src2,const char s = '.',int32_t start = 0)
        {
          if((1 > buffer_size_ ) ||(src1.length() + start + src2.length() > buffer_size_))
            return false;
          if(start >= 0 && start <= data_length_)
             data_length_ = start;
          else
          {
            TBSYS_LOG(WARN,"concat input [start:%d],but data_length is:%d, not valid",start,data_length_);
            return false;
          }
          if(src1.length() > 0)
          {
            memcpy(ptr_ + start ,src1.ptr(), src1.length());
            data_length_ += src1.length();
            if(s != '\0')
            {
              *(ptr_ + start + src1.length()) = s;
              data_length_ += 1;
            }

          }
          if(src2.length() >0 )
          {
            memcpy(ptr_  + data_length_,src2.ptr(),src2.length());
            data_length_ += src2.length();
          }
          //last we update the dest length,so we add this function as a private call

          //set_length(src1.length() + src2.length() + start + 1);
          return true;
        }

        inline bool concat(const char* src1,const char* src2,const char c = '.',int32_t start=0)
        {
          ObString s1(0, static_cast<int32_t>(strlen(src1)),src1);
          ObString s2(0, static_cast<int32_t>(strlen(src2)),src2);
          return concat(s1,s2,c,start);
        }

      inline const char* find(const char c) const
      {
        const char* ret = NULL;
        if (NULL != ptr_)
        {
          for(obstr_size_t i=0; i < data_length_; ++i)
          {
            if (ptr_[i] == c)
            {
              ret = &ptr_[i];
              break;
            }
          }
        }
        return ret;
      }
        /**
         * @param the split character
         * @param dest1 the first part if we split src
         * @param dest2 the second part if we split src
         * @param buf1  the dest1 capacity can hold characters
         * @param buf2  the dest2 capactiy can hold characters
         * @return 0 if src has no split character s,or the first dest length
         */
        inline bool split_two(ObString& dest1,ObString& dest2,const char split = '.') const
        {
          const char* ret = find(split);
          int32_t  buf1 = dest1.buffer_size_;
          int32_t  buf2 = dest2.buffer_size_;
          if( buf1 + buf2 < length() -1 )
          {
            return false;
          }
          if(ret == NULL)
          {
            //*(dest1.ptr()) = '';
            dest1.set_length(0);
            memcpy(dest2.ptr(),ptr(),data_length_);
            dest2.set_length(data_length_);
            return  true;
          }
          else
          {
            int32_t len = static_cast<int32_t>(ret - ptr());
            if(len >0 && buf1 >= len && buf2 >= data_length_ - len -1 )
            {
              memcpy(dest1.ptr(),ptr(), len);
              dest1.set_length(len);
              memcpy(dest2.ptr(),ret+1,length() - len - 1 );
              dest2.set_length(length() - len -1);
              return true;
            }
            else
            {
              return false;
            }

          }

        }
        inline int32_t set_length(int32_t l)
        {
          if(l >= 0)
          {
            data_length_ = l;
            return data_length_;
          }
          else
            return 0;
        }
        //add:e
        inline bool operator<(const ObString& obstr) const
        {
          return compare(obstr) < 0;
        }

        inline bool operator<=(const ObString& obstr) const
        {
          return compare(obstr) <= 0;
        }

        inline bool operator>(const ObString& obstr) const
        {
          return compare(obstr) > 0;
        }

        inline bool operator>=(const ObString& obstr) const
        {
          return compare(obstr) >= 0;
        }

        inline bool operator==(const ObString& obstr) const
        {
          return compare(obstr) == 0;
        }

        inline bool operator!=(const ObString& obstr) const
        {
          return compare(obstr) != 0;
        }

        inline bool operator<(const char *str) const
        {
          return compare(str) < 0;
        }

        inline bool operator<=(const char *str) const
        {
          return compare(str) <= 0;
        }

        inline bool operator>(const char *str) const
        {
          return compare(str) > 0;
        }

        inline bool operator>=(const char *str) const
        {
          return compare(str) >= 0;
        }

        inline bool operator==(const char *str) const
        {
          return compare(str) == 0;
        }

        inline bool operator!=(const char *str) const
        {
          return compare(str) != 0;
        }

        static ObString make_string(const char* cstr)
        {
          ObString ret(0, static_cast<obstr_size_t>(strlen(cstr)), const_cast<char*>(cstr));
          return ret;
        }

        int64_t to_string(char *buff, const int64_t len) const
        {
          int64_t pos = 0;
          pos = snprintf(buff, len, "%.*s", length(), ptr());
          if (pos < 0)
          {
            pos = 0;
          }
          return pos;
        }

        friend std::ostream & operator<<(std::ostream &os, const ObString& str); // for google test
      private:
        obstr_size_t buffer_size_;
        obstr_size_t data_length_;
        char* ptr_;
    };

    DEFINE_SERIALIZE(ObString)
    {
      int res = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();
      if (buf == NULL || serialize_size > buf_len - pos)
      {
        res = OB_ERROR;
      }

      if (res == OB_SUCCESS)
      {
        //Null ObString is allowed
        res = serialization::encode_vstr(buf, buf_len, pos,ptr_, data_length_);
      }
      return res;
    }

    DEFINE_DESERIALIZE(ObString)
    {
      int res = OB_SUCCESS;
      int64_t len = 0;
      if ( NULL == buf || (data_len - pos) < 2)  //at least need two bytes
      {
        res = OB_ERROR;
      }
      if (OB_SUCCESS == res)
      {
        if ( 0 == buffer_size_)
        {
          ptr_ = const_cast<char*>(serialization::decode_vstr(buf,data_len,pos,&len));
          if (NULL == ptr_)
          {
            res = OB_ERROR;
          }
        }
        else
        {
          //copy to ptr_
          int64_t str_len = serialization::decoded_length_vstr(buf,data_len,pos);
          if (str_len < 0 || buffer_size_ < str_len || (data_len - pos) < str_len)
          {
            res = OB_ERROR;
          }
          else if (NULL == serialization::decode_vstr(buf,data_len,pos,ptr_,buffer_size_,&len))
          {
            res = OB_ERROR;
          }
        }
        data_length_ = static_cast<obstr_size_t>(len);
      }
      return res;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObString)
    {
      return serialization::encoded_length_vstr(data_length_);
    }

    inline std::ostream & operator<<(std::ostream &os, const ObString& str) // for google test
    {
      os << "size=" << str.buffer_size_ << " len=" << str.data_length_;
      return os;
    }

    template <typename AllocatorT>
    int ob_write_string(AllocatorT &allocator, const ObString &src, ObString& dst)
    {
      int ret = OB_SUCCESS;
      int32_t src_len = src.length();
      void * ptr = NULL;
      if (OB_UNLIKELY(NULL == src.ptr() || 0 >= src_len))
      {
        dst.assign(NULL, 0);
      }
      else if (NULL == (ptr = allocator.alloc(src_len)))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memcpy(ptr, src.ptr(), src_len);
        dst.assign_ptr(reinterpret_cast<char*>(ptr), src_len);
      }
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160504:b
    template <typename AllocatorT>
    int ob_write_string(AllocatorT &allocator, const ObString &src, ObString& dst, int32_t start_pos, int32_t end_pos)
    {
      int ret = OB_SUCCESS;
      int32_t src_len = src.length();
      void * ptr = NULL;
      if (OB_UNLIKELY(NULL == src.ptr() || 0 >= src_len))
      {
        dst.assign(NULL, 0);
      }
      else if (start_pos < 0 || end_pos <= start_pos || end_pos >= src_len)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (ptr = allocator.alloc(end_pos-start_pos)))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memcpy(ptr, src.ptr()+start_pos, end_pos-start_pos);
        dst.assign_ptr(reinterpret_cast<char*>(ptr), end_pos-start_pos);
      }
      return ret;
    }
    //add:e
  }
}
#endif
