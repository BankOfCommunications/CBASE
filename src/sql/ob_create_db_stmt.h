/*
 * add wenghaixing [database manage]20150608
 * create database ,put database name into statement
 * */
#ifndef OB_CREATE_DB_STMT_H
#define OB_CREATE_DB_STMT_H

#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObCreateDbStmt: public ObBasicStmt
    {
      public:
        ObCreateDbStmt();
        virtual ~ObCreateDbStmt();

        virtual void print(FILE* fp, int32_t level, int32_t index);

        int add_database(const common::ObString &database);
        const common::ObStrings* get_database() const;

      public:
        ObCreateDbStmt(const ObCreateDbStmt &other);
        ObCreateDbStmt& operator =(const ObCreateDbStmt &other);

      private:
        common::ObStrings database_;
    };
  } //end namespace sql
} //end namespace oceanbase

#endif // OB_CREATE_DB_STMT_H
