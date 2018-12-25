/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_sub_get_request.h for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MS_SUB_GET_REQUEST_H_
#define MERGESERVER_OB_MS_SUB_GET_REQUEST_H_
#include "common/ob_get_param.h"
#include "common/ob_scanner.h"
#include "common/ob_iterator.h"
#include "ob_ms_rpc_event.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerSubGetRequest
    {
    public:
      ObMergerSubGetRequest();
      ~ObMergerSubGetRequest()
      {
      }
      void init(common::PageArena<int64_t, common::ModulePageAllocator> & mem_allocator);
      void reset();

      void set_param(common::ObGetParam & get_param);
      int add_cell(const int64_t cell_idx);
      int add_result(common::ObScanner &res);

      int has_next();
      int  next(common::ObInnerCellInfo *&cell, int64_t & org_cell_idx);
      int reset_iterator();

      inline void set_last_svr_ipv4(const int32_t svr_ip)
      {
        svr_ip_ = svr_ip;
        if (fail_svr_ip_ != svr_ip_)
        {
          fail_svr_ip_ = 0;
        }
      }

      inline uint32_t get_last_svr_ipv4()const
      {
        return svr_ip_;
      }

      inline void set_last_rpc_event(ObMergerRpcEvent & rpc_event, const int64_t session_id = -1)
      {
        last_rpc_event_ = rpc_event.get_event_id();
        last_session_id_ = session_id;
      }

      inline uint64_t get_last_rpc_event()const
      {
        return last_rpc_event_;
      }

      inline int64_t get_retry_times()const
      {
        return retry_times_;
      }

      inline int64_t retry()
      {
        fail_svr_ip_ = svr_ip_;
        return(++retry_times_);
      }

      inline uint32_t get_fail_svr_ipv4()const
      {
        return fail_svr_ip_;
      }

      inline bool finished()const
      {
        return (received_cell_count_ >= cell_idx_in_org_param_.size());
      }

      inline int get_cur_param(common::ObGetParam *& get_param)const
      {
        int err = common::OB_SUCCESS;
        get_param = NULL;
        if (common::OB_SUCCESS == (err = get_next_param_(cur_get_param_)))
        {
          get_param = &cur_get_param_;
        }
        return err;
      }

      inline void clear_get_param(void)
      {
        cur_get_param_.destroy();
      }

      inline common::ObCellInfo * get_first_unfetched_cell()const
      {
        common::ObCellInfo *cell = NULL;
        if ((NULL != pget_param_) && (received_cell_count_ < cell_idx_in_org_param_.size()))
        {
          cell = (*pget_param_)[static_cast<int32_t>(cell_idx_in_org_param_[static_cast<int32_t>(received_cell_count_)])];
        }
        return cell;
      }

      inline int64_t get_last_session_id()const
      {
        return last_session_id_;
      }

    private:
      int get_next_param_(common::ObGetParam & get_param)const;
      common::ObGetParam *pget_param_;
      mutable common::ObGetParam cur_get_param_;
      typedef common::ObVector<int64_t, common::PageArena<int64_t, common::ModulePageAllocator> > IVec;
      IVec cell_idx_in_org_param_;
      IVec res_vec_;
      int64_t   received_cell_count_;

      int64_t   res_iterator_idx_;
      int64_t   poped_cell_count_;
      uint32_t   svr_ip_;
      uint32_t   fail_svr_ip_;
      uint64_t last_rpc_event_;
      int64_t   last_session_id_;
      int64_t retry_times_;
      common::ObStringBuf rowkey_buf_;
      common::ObRowkey last_rowkey_;
    };

    class ObGetMerger : public common::ObInnerIterator
    {
    public:
      ObGetMerger()
      {
        res_vec_ = NULL;
        res_size_ = 0;
        heap_size_ = 0;
        cur_cell_idx_ = 0;
      }
      ~ObGetMerger()
      {
      }
      int init(ObMergerSubGetRequest * results, const int64_t res_count, const common::ObGetParam & get_param);

      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);
    private:
      static const int64_t MAX_RES_SIZE = 4096;
      struct ResInfo
      {
        int64_t org_cell_idx_;
        int64_t res_idx_;
        common::ObInnerCellInfo *cur_cell_;
        bool operator<(const ResInfo &other)
        {
          return org_cell_idx_ > other.org_cell_idx_;
        }
      };
      const common::ObGetParam            *get_param_;
      ResInfo               info_heap_[MAX_RES_SIZE];
      int64_t               heap_size_;
      ObMergerSubGetRequest *res_vec_;
      int64_t res_size_;

      common::ObInnerCellInfo     cur_cell_;
      int64_t               cur_cell_idx_;
    };
  }
}

#endif /* MERGESERVER_OB_MS_SUB_GET_REQUEST_H_ */
