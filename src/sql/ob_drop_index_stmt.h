#ifndef OB_DROP_INDEX_STMT_H
#define OB_DROP_INDEX_STMT_H
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "parse_node.h"
namespace oceanbase
{
  namespace sql
  {
    class ObDropIndexStmt : public ObBasicStmt
    {
    public:
      explicit ObDropIndexStmt(common::ObStringBuf* name_pool);
      virtual ~ObDropIndexStmt();

      void set_if_exists(bool if_not_exists);
      int add_table_name_id(ResultPlan& result_plan, const common::ObString& table_name);

      bool get_if_exists() const;
      int64_t get_table_size() const;
      const common::ObString& get_table_name(int64_t index) const;
      void print(FILE* fp, int32_t level, int32_t index = 0);
      int64_t get_table_count() const;
      uint64_t get_table_id(int64_t index) const;
      uint64_t get_data_table_id(int64_t index) const;//add liumz, [bugfix:drop index privilege management]20150828
    //protected:
    //  void print_indentation(FILE* fp, int32_t level);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      bool  if_exists_;
      common::ObArray<common::ObString>   tables_;
      common::ObArray<uint64_t> table_ids_;
      common::ObArray<uint64_t> data_table_ids;//add liumz, [bugfix:drop index privilege management]20150828
    };

    inline int64_t ObDropIndexStmt::get_table_count() const
    {
      return table_ids_.count();
    }
    inline uint64_t ObDropIndexStmt::get_table_id(int64_t index) const
    {
      OB_ASSERT( 0 <= index && index < table_ids_.count());
      return table_ids_.at(index);
    }
    //add liumz, [bugfix:drop index privilege management]20150828:b
    inline uint64_t ObDropIndexStmt::get_data_table_id(int64_t index) const
    {
      OB_ASSERT( 0 <= index && index < data_table_ids.count());
      return data_table_ids.at(index);
    }
    //add:e
    inline void ObDropIndexStmt::set_if_exists(bool if_exists)
    {
      if_exists_ = if_exists;
    }

    inline bool ObDropIndexStmt::get_if_exists() const
    {
      return if_exists_;
    }

    inline int64_t ObDropIndexStmt::get_table_size() const
    {
      return tables_.count();
    }

    inline const common::ObString& ObDropIndexStmt::get_table_name(int64_t index) const
    {
      OB_ASSERT(0 <= index && index < tables_.count());
      return tables_.at(index);
    }
  }
}
#endif // OB_DROP_INDEX_STMT_H
