/*
 *add wenghaixing [secondary index drop index]20141222
 *
 *This source file is used to drop index table.
 */
#ifndef OB_DROP_INDEX_H
#define OB_DROP_INDEX_H
#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_strings.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObDropIndex: public ObNoChildrenPhyOperator
    {
      public:
        ObDropIndex();
        virtual ~ObDropIndex();
        virtual void reset();
        virtual void reuse();
        // init
        void set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc);
        int add_index_name (const common::ObString &tname);

        /// execute the insert statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_DROP_INDEX; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObDropIndex(const ObDropIndex &other);
        ObDropIndex& operator=(const ObDropIndex &other);
        // function members
      private:
        // data members
        mergeserver::ObMergerRootRpcProxy* rpc_;
        common::ObStrings indexs_;
    };

    inline int ObDropIndex::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObDropIndex::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

    inline void ObDropIndex::set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc)
    {
      rpc_ = rpc;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif // OB_DROP_INDEX_H
