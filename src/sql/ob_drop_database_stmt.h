/*
 * Drop database stmt
 * add wenghaixing [database manage]
 * */
#ifndef OB_DROP_DATABASE_STMT_H
#define OB_DROP_DATABASE_STMT_H

#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDropDbStmt: public ObBasicStmt
    {
      public:
        ObDropDbStmt();
        virtual ~ObDropDbStmt();

        virtual void print(FILE *fp, int32_t level, int32_t index);

        int add_database(const common::ObString &database);
        const common::ObStrings* get_database()const;

      public:
        ObDropDbStmt(const ObDropDbStmt &other);
        ObDropDbStmt& operator = (const ObDropDbStmt &other);

      private:
        common::ObStrings database_;
    };
  }//end namespace sql
}//end namespace oceanbase



#endif // OB_DROP_DATABASE_STMT_H
