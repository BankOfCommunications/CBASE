#ifndef OB_AGENT_SCAN_H
#define OB_AGENT_SCAN_H
#include "sql/ob_no_children_phy_operator.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_range2.h"
#include "common/ob_server.h"
#include "common/ob_array.h"
#include "sql/ob_sstable_scan.h"
#include "sql/ob_tablet_scan.h"
#include "ob_cs_query_agent.h"
///这个操作符专门用于CS之间的交互，如果遇到跨CS的情况就使用ObQueryAgent
///如果遇到本机的CS数据就使用ObSSTableScan
namespace oceanbase
{
  using namespace oceanbase::common;
  using namespace oceanbase::sql;
  namespace chunkserver
  {
   // typedef hash::ObHashMap<ObNewRange,ObServer,hash::NoPthreadDefendMode> RangeServerHash;
   // typedef hash::ObHashMap<ObNewRange,ObServer,hash::NoPthreadDefendMode>::const_iterator HashIterator;
    class ObAgentScan : public ObNoChildrenPhyOperator
    {
      public:
        ObAgentScan();
        ~ObAgentScan();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_AGENT_SCAN; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note 调用
        virtual int get_next_row(const ObRow *&row);
        virtual int get_row_desc(const ObRowDesc *&row_desc) const;

        void set_row_desc(const ObRowDesc& desc);

      public:
        void set_query_agent(chunkserver::ObCSQueryAgent *agent);
        int build_sst_scan_param();
        int get_next_local_row(const ObRow *&row);
        int scan();
        int get_next_local_range(ObNewRange &range);
        void set_server(ObServer server){self_ = server;}
        void set_scan_context(ScanContext &sc){sc_ = sc;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        ///请注意，这个操作符不需要用到序列化函数!!!
      private:
        ObRowDesc                         row_desc_;
        ObRow                             cur_row_;
        //RangeServerHash* range_server_;
        chunkserver::ObCSQueryAgent       *query_agent_;
        ObSSTableScan                     sst_scan_;
        sstable::ObSSTableScanParam       sst_scan_param_;
        const chunkserver::RangeServerHash      *range_server_hash_;
        int64_t                           hash_index_;
        bool                              local_idx_scan_finishi_;
        bool                              local_idx_block_end_;
        bool                              first_scan_;
        ObServer                          self_;
        ScanContext                       sc_;
        //int64_t                           row_count_;

    };
  }//end sql
} //end oceanbase

#endif // OB_AGENT_SCAN_H
