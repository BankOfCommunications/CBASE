#ifndef OB_USE_DB_STMT_H
#define OB_USE_DB_STMT_H

#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObUseDbStmt: public ObBasicStmt
    {
      public:
        ObUseDbStmt();
        virtual ~ObUseDbStmt();

        virtual void print(FILE* fp, int32_t level, int32_t index);
        int add_database(const common::ObString &database);
        const common::ObStrings* get_database() const;

      public:
        ObUseDbStmt(const ObUseDbStmt &other);
        ObUseDbStmt& operator =(const ObUseDbStmt &other);

      private:
        common::ObStrings database_;
    };
  }//end namespace sql
}//end namespace oceanbase

#endif // OB_USE_DB_STMT_H
