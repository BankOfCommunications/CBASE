/*
 * =====================================================================================
 *
 *       Filename:  DbTableInfo.h
 *
 *        Version:  1.0
 *        Created:  04/13/2011 09:57:25 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OB_API_DBTABLEINFO_H
#define  OB_API_DBTABLEINFO_H

#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "db_record.h"
#include <string>

namespace oceanbase {
    namespace api {
        const int k2M = 1024 * 1024 * 2;
        //mod by qianzm 20160226:b
		//const int kTabletDupNr = 3;
		const int kTabletDupNr = 6;
		//mod 20160226:e

        using namespace oceanbase::common;

        class DbRecord;
        struct TabletSliceLocation {
            TabletSliceLocation()
              :ip_v4(0), cs_port(0), ms_port(0), tablet_version(0), server_avail(false) { }

            int32_t ip_v4;
            unsigned short cs_port;
            unsigned short ms_port;
            int64_t tablet_version;
            bool server_avail;
        };

        struct TabletStats {
          TabletStats() : hit_times(0) { }

          int64_t hit_times;
        };

        class RefBuffer {
          public:
            RefBuffer() : ptr_(NULL), ref_(NULL) { }

            ~RefBuffer() {
              unref();
            }

            RefBuffer(const RefBuffer &other) {
              if (other.ref_ != NULL) {
                __sync_add_and_fetch(&other.ref_->ref_count, 1);
                ptr_ = other.ptr_;
                ref_ = other.ref_;
              } else {
                ptr_ = NULL;
                ref_ = NULL;
              }
            }

            RefBuffer& operator=(const RefBuffer &other) {
              if (ptr_ != other.ptr_) {
                RefBuffer tmp(other);
                swap(tmp);
              }
              return *this;
            }

            inline char* alloc(int64_t sz) {
              ptr_ = (char *)ob_malloc(sz, ObModIds::TEST);
              if (ptr_ != NULL) {
                ref();
              }
              return ptr_;
            }

            inline void free(char* ptr) {
              ob_free(ptr);
            }

            void swap(RefBuffer &other) {
              std::swap(ptr_, other.ptr_);
              std::swap(ref_, other.ref_);
            }
          private:
            struct RefCtrlBlock {
              int64_t ref_count;
              RefCtrlBlock() : ref_count(1) { }
            };

            void ref() {
              if (ref_ != NULL) {
                __sync_add_and_fetch(&ref_->ref_count, 1);
              } else {
                ref_ = new(std::nothrow) RefCtrlBlock();
              }
            }

            void unref() {
              if (ref_ != NULL) {
                int64_t ref_count = __sync_add_and_fetch(&ref_->ref_count, -1);
                if (ref_count <= 0) {
                  delete ref_;
                  free(ptr_);

                  ref_ = NULL;
                  ptr_ = NULL;
                }
              }
            }
          private:
            char *ptr_;
            RefCtrlBlock *ref_;
        };

        class TabletInfo {
          public:
              TabletInfo(void);
              TabletInfo(const TabletInfo &src);

              ~TabletInfo();

              TabletInfo &operator=(const TabletInfo &src);

              void dump_slice(void);

              int get_tablet_location(int idx, TabletSliceLocation &loc);

              int parse_from_record(DbRecord *recp);

              int get_one_avail_slice(TabletSliceLocation &loc, const ObRowkey &rowkey);

              TabletSliceLocation slice_[kTabletDupNr];

              const ObRowkey &get_end_key() const;

              int assign_end_key(const ObRowkey &key);

              TabletStats stats() const { return stats_; }

              bool expired(uint64_t timeout) const;
              int64_t ocuppy_size_;
              int64_t record_count_;
          private:
              int parse_one_cell(const common::ObCellInfo *cell);
              int check_validity();

              uint64_t timestamp_;
              TabletStats stats_;
              //ObMemBuf rowkey_buffer_;
              RefBuffer rowkey_buffer_;
              ObRowkey rowkey_;
        };
    }
}
#endif
