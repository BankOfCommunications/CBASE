/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_config_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_CONFIG_PROVIDER_H
#define _OB_SQL_CONFIG_PROVIDER_H 1

namespace oceanbase
{
  namespace sql
  {
    class ObSQLConfigProvider
    {
      public:
        ObSQLConfigProvider(){};
        virtual ~ObSQLConfigProvider(){};

        virtual bool is_read_only() const = 0;
        virtual bool is_regrant_priv() const = 0;//add liumz, [multi_database.priv_manage]20150708
      private:
        // types and constants
      private:
        // disallow copy
        ObSQLConfigProvider(const ObSQLConfigProvider &other);
        ObSQLConfigProvider& operator=(const ObSQLConfigProvider &other);
        // function members
      private:
        // data members
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_CONFIG_PROVIDER_H */
