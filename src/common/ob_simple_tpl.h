#include <errno.h>
#include <new>
#include "serialization.h"
#include "ob_object.h"
#include "ob_string.h"
#include "ob_rowkey.h"

#ifndef __OB_COMMON_OB_SIMPLE_TPL_H__
#define __OB_COMMON_OB_SIMPLE_TPL_H__

namespace oceanbase
{
  namespace common
  {
    template<typename Type1, typename Type2>
    struct Pair
    {
      Type1* car_;
      Type2* cdr_;
      Pair(): car_(NULL), cdr_(NULL) {}
      Pair(Type1* car, Type2* cdr): car_(car), cdr_(cdr) {}
      ~Pair() {}
      int serialize(char* buf, const int64_t len, int64_t& pos) const
      {
        int err = OB_SUCCESS;
        int64_t new_pos = pos;
        if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, new_pos, (int64_t)car_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(car)=>%d", err);
        }
        else if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, new_pos, (int64_t)cdr_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(cdr)=>%d", err);
        }
        else
        {
          pos = new_pos;
        }
        return err;
      }

      int deserialize(const char* buf, const int64_t data_len, int64_t& pos)
      {
        int err = OB_SUCCESS;
        int64_t new_pos = pos;
        if(OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&car_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(car)=>%d", err);
        }
        else if(OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&cdr_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(cdr)=>%d", err);
        }
        else
        {
          pos = new_pos;
        }
        return err;
      }
    };

    template<typename Factory>
    struct TSI
    {
      public:
        typedef typename Factory::InstanceType T;        
      public:
        TSI(Factory& factory): create_err_(0), factory_(factory) {
          create_err_ = pthread_key_create(&key_, destroy);
        }
        ~TSI() {
          if (0 == create_err_)
            pthread_key_delete(key_);
        }
        T* get(){
          int err = 0;
          T* val = NULL;
          if (create_err_ != 0)
          {}
          else if (NULL != (val = (T*)pthread_getspecific(key_)))
          {}
          else if (NULL == (val = new(std::nothrow)T()))
          {}
          else if (0 != (err = factory_.init_instance(val)))
          {}
          else if (0 != (err = pthread_setspecific(key_, val)))
          {}
          if (0 != err)
          {
            destroy(val);
            val = NULL;
          }
          return val;
        }
      private:
        static void destroy(void* arg) {
          if (NULL != arg)
          {
            delete (T*)arg;
          }
        }
        int create_err_;
        pthread_key_t key_;
        Factory& factory_;
    };

// 不能删除，不管内存
    template<typename T>
    class SimpleList
    {
      public:
        SimpleList(): tail_((T*)&guard_) { tail_->next_ = tail_; }
        ~SimpleList() {}
        int add(T* node) {
          int err = OB_SUCCESS;
          node->next_ = tail_->next_;
          tail_->next_ = node;
          tail_ = node;
          return err;
        }
        T* begin() { return tail_->next_->next_; }
        T* end() { return tail_->next_; }
        const T* begin() const { return tail_->next_->next_; }
        const T* end() const { return tail_->next_; }
      private:
        T* guard_;
        T* tail_;
    };

    template<typename Allocator>
    class ObHandyAllocatorWrapper: public Allocator
    {
      public:
        ObHandyAllocatorWrapper(): allocated_(0) {}
        ~ObHandyAllocatorWrapper() {}
      private:
        volatile int64_t allocated_;
      public:
        int64_t get_alloc_size() const { return allocated_; }
        void* alloc(const int64_t size)
        {
          int err = OB_SUCCESS;
          void* p = NULL;
          if (NULL == (p = Allocator::alloc(size)))
          {
            err = OB_MEM_OVERFLOW;
          }
          else
          {
            __sync_fetch_and_add(&allocated_, size);
          }
          return p;
        }
        template<typename T>
        T* new_obj()
        {
          T* p = NULL;
          if (NULL != (p = alloc(sizeof(T))))
          {
            new(p)T();
          }
          return p;
        }

        int write_string(const ObString& str, ObString* stored_str)
        {
          return NULL != stored_str ? write_string(str, *stored_str): OB_SUCCESS;
        }

        int write_obj(const ObObj& obj, ObObj* stored_obj)
        {
          return NULL != stored_obj ? write_obj(obj, *stored_obj): OB_SUCCESS;
        }

        int write_string(const ObString& str, ObString& stored_str)
        {
          int err = OB_SUCCESS;
          char* p = NULL;
          if (0 == str.length() || NULL == str.ptr())
          {
            stored_str.assign_ptr(NULL, 0);
          }
          else if(NULL == (p = (char*)alloc(str.length())))
          {
            err = OB_MEM_OVERFLOW;
            TBSYS_LOG(ERROR, "alloc(%d)=>%d", str.length(), err);
          }
          else
          {
            memcpy(p, str.ptr(), str.length());
            stored_str.assign_ptr(p, str.length());
          }
          return err;
        }

        int write_string(const ObRowkey& rowkey, ObRowkey* stored_rowkey)
        {
          return NULL != stored_rowkey ? write_string(rowkey, *stored_rowkey): OB_SUCCESS;
        }

        int write_string(const ObRowkey& rowkey, ObRowkey& stored_rowkey)
        {
          int err = OB_SUCCESS;
          char* p = NULL;
          int64_t len = rowkey.get_deep_copy_size();
          if (0 == rowkey.length() || NULL == rowkey.ptr())
          {
            stored_rowkey.assign(NULL, 0);
          }
          else if (NULL == (p = (char*)alloc(len)))
          {
            err = OB_MEM_OVERFLOW;
            TBSYS_LOG(ERROR, "alloc(len=%ld)=>%d", len, err);
          }
          else
          {
            ObRawBufAllocatorWrapper allocator(p, len);
            err = rowkey.deep_copy(stored_rowkey, allocator);
          }
          return err;
        }

        int write_obj(const ObObj& obj, ObObj& stored_obj)
        {
          int err = OB_SUCCESS;
          ObString value;
          ObString new_value;
          stored_obj = obj;
          if (ObVarcharType != obj.get_type())
          {}
          else if (OB_SUCCESS != (err = obj.get_varchar(value)))
          {
            TBSYS_LOG(ERROR, "obj.get_varchar()=>%d", err);
          }
          else if (OB_SUCCESS != (err = write_string(value, new_value)))
          {
            TBSYS_LOG(ERROR, "write_string(%*s)=>%d", value.length(), value.ptr(), err);
          }
          else
          {
            stored_obj.set_varchar(new_value);
          }
          return err;
        }
    };
  }; // end namespace common
}; // end namespace oceanbase
#endif /* __OB_COMMON_OB_SIMPLE_TPL_H__ */
