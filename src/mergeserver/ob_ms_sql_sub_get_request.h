/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_sql_sub_get_request.h for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   xiaochu <xiaochu.yh@alipay.com>
 */
#ifndef MERGESERVER_OB_MS_SQL_SUB_GET_REQUEST_H_
#define MERGESERVER_OB_MS_SQL_SUB_GET_REQUEST_H_
#include "sql/ob_sql_get_param.h"
#include "common/ob_new_scanner.h"
#include "common/ob_row_iterator.h"
#include "ob_ms_sql_rpc_event.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsSqlSubGetRequest;
    typedef common::ObVector<ObMsSqlSubGetRequest*> SubRequestVector;

    class ObMsSqlSubGetRequest
    {
    public:
      ObMsSqlSubGetRequest();
      ~ObMsSqlSubGetRequest()
      {
      }
      void init(common::PageArena<int64_t, common::ModulePageAllocator> & mem_allocator);
      void reset();

      void set_param(sql::ObSqlGetParam & get_param);
      inline void set_row_desc(const common::ObRowDesc &desc)
      {
        cur_row_.set_row_desc(desc);
      }

      int add_row(const int64_t row_idx);
      int add_result(common::ObNewScanner &res);

      int get_next_row(oceanbase::common::ObRow &row, int64_t & org_row_idx);
      int get_next_row(oceanbase::common::ObRow *&row, int64_t & org_row_idx);

      inline void set_first_cs_addr(const ObServer & server)
      {
        first_cs_addr_ = server;
      }

      inline ObServer & get_first_cs_addr()
      {
        return first_cs_addr_;
      }

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

      inline void set_last_rpc_event(ObMsSqlRpcEvent & rpc_event, const int64_t session_id = -1)
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
        return (received_row_count_ >= row_idx_in_org_param_.size());
      }

      inline int get_cur_param(sql::ObSqlGetParam & get_param)const
      {
        int err = common::OB_SUCCESS;
        if (common::OB_SUCCESS != (err = get_next_param_(get_param)))
        {
          TBSYS_LOG(WARN, "fail to get get next param. err=%d", err);
        }
        return err;
      }

      inline const common::ObRowkey *get_first_unfetched_row()const
      {
        const common::ObRowkey *rowkey = NULL;
        if ((NULL != pget_param_) && (received_row_count_ < row_idx_in_org_param_.size()))
        {
          rowkey = (*pget_param_)[static_cast<int32_t>(row_idx_in_org_param_[static_cast<int32_t>(received_row_count_)])];
        }
        return rowkey;
      }

      inline int64_t get_last_session_id()const
      {
        return last_session_id_;
      }
    private:
      int get_next_param_(sql::ObSqlGetParam & get_param)const;
      //指向整个大请求的指针
      sql::ObSqlGetParam *pget_param_;
      
      /* key data structure of ObMsSqlSubGetRequest */
      typedef common::ObVector<int64_t, common::PageArena<int64_t, common::ModulePageAllocator> > IVec;
      IVec row_idx_in_org_param_;
      IVec res_vec_;

      /* record how many scanners consumed */
      int64_t   res_iterator_idx_;
      /* record how many rows received */
      int64_t   received_row_count_;
      /* record how many rows consumed */
      int64_t   poped_row_count_;
      /* @TODO: for retry purpose, need optimize out */
      uint32_t   svr_ip_;
      uint32_t   fail_svr_ip_;
      /* record some request related info */
      uint64_t last_rpc_event_;
      int64_t   last_session_id_;
      int64_t retry_times_;
      common::ObRow cur_row_;
      common::ObStringBuf rowkey_buf_;
      ObServer first_cs_addr_;
    };

    class ObSqlGetMerger : public common::ObRowIterator
    {
    public:
      ObSqlGetMerger()
      {
        res_vec_ = NULL;
        res_size_ = 0;
        heap_size_ = 0;
        cur_row_idx_ = 0;
      }
      ~ObSqlGetMerger()
      {
      }
      int init(SubRequestVector *results, const int64_t res_count, const sql::ObSqlGetParam & get_param);
      int get_next_row(common::ObRow &row);
    private:
      static const int64_t MAX_RES_SIZE = 4096;
      struct ResInfo
      {
        int64_t org_row_idx_;
        int64_t res_idx_;
        common::ObRow *cur_row_;
        bool operator<(const ResInfo &other)
        {
          return org_row_idx_ > other.org_row_idx_;
        }
      };
      /* rowkey source indexed by cur_row_idx_ */
      const sql::ObSqlGetParam            *get_param_;
      /* used to find next output row */
      ResInfo               info_heap_[MAX_RES_SIZE];
      int64_t               heap_size_;
      /* have all sub get request collection */
      //ObMsSqlSubGetRequest *res_vec_;
      common::ObVector<ObMsSqlSubGetRequest *> *res_vec_;
      int64_t res_size_;
      /* for valid check */
      int64_t               cur_row_idx_;
      bool    has_read_first_row_;
    };
  }
}

#endif /* MERGESERVER_OB_MS_SQL_SUB_GET_REQUEST_H_ */
