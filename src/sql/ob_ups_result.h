/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_result.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef _OB_UPS_RESULT_H
#define _OB_UPS_RESULT_H 1
#include "common/ob_allocator.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_new_scanner.h"
#include "common/ob_transaction.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    class PageArenaAllocator : public ObIAllocator
    {
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L;
      public:
        PageArenaAllocator() : mod_(ObModIds::OB_UPS_RESULT), arena_(ALLOCATOR_PAGE_SIZE, mod_) {};
        ~PageArenaAllocator() {};
      public:
        void *alloc(const int64_t sz) {return arena_.alloc(sz);};
        void free(void *ptr) {arena_.free((char*)ptr);};
        void reuse() {arena_.reuse();};
        void set_mod_id(int32_t mod_id) {mod_.set_mod_id(mod_id);};
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena arena_;
    };

    class ObUpsResult
    {
      static const uint64_t F_UPS_RESULT_START = 0x7570735f725f7631; // "ups_r_v1"
      static const uint8_t F_UPS_RESULT_END = 0xff;
      static const uint8_t F_ERROR_CODE = 0x01;
      static const uint8_t F_AFFECTED_ROWS = 0x02;
      static const uint8_t F_WARNING_LIST = 0x03;
      static const uint8_t F_NEW_SCANNER = 0x04;
      static const uint8_t F_TRANS_ID = 0x05;
      struct StringNode
      {
        StringNode *next;
        int64_t length;
        char buf[0];
      };
      public:
        ObUpsResult();
        explicit ObUpsResult(ObIAllocator &allocator);
        ~ObUpsResult();
      public:
        void set_error_code(const int error_code);
        void set_affected_rows(const int64_t affected_rows);
        void add_warning_string(const char *buffer);
        int set_scanner(const ObNewScanner &scanner);
        void set_trans_id(const ObTransID &trans_id) {trans_id_ = trans_id;};
      public:
        int get_error_code() const;
        int64_t get_affected_rows() const;
        int64_t get_warning_count() const;
        const char *get_next_warning();
        void reset_iter_warning();
        ObNewScanner &get_scanner();
        const ObTransID &get_trans_id() const {return trans_id_;};
      public:
        void clear();
      public:
        int serialize_warning_list_(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize_warning_list_(const char* buf, const int64_t data_len, int64_t& pos);
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        PageArenaAllocator default_allocator_;
        ObIAllocator &allocator_;
        int error_code_;
        int64_t affected_rows_;
        StringNode *warning_list_;
        StringNode *warning_iter_;
        int64_t warning_count_;
        ObNewScanner scanner_;
        char *scanner_sel_buffer_;
        int64_t scanner_sel_length_;
        ObTransID trans_id_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_RESULT_H */
