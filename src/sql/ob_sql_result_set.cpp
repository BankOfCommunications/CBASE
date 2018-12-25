/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_sql_result_set.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */


#include "tbsys.h"
#include "common/ob_new_scanner.h"
#include "common/serialization.h"
#include "sql/ob_result_set.h"
#include "ob_sql_result_set.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::sql;

int ObSQLResultSet::serialize(char *buf,
                              const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t pos_bk = pos;
  const ObIArray<ObResultSet::Field> &fields = rs_.get_field_columns();
  const ObRow *row = NULL;

  if (OB_SUCCESS != (ret = encode_vi32(buf, len, pos, errno_)))
  {
    TBSYS_LOG(ERROR, "encode errno error, ret = [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = sqlstr_.serialize(buf, len, pos)))
  {
    TBSYS_LOG(ERROR, "encode sqlstr error, ret = [%d]", ret);
  }
  else if (OB_SUCCESS != errno_)
  {
    TBSYS_LOG(WARN, "execute sqlstr error, errno: [%d], sql[%.*s]",
              errno_, sqlstr_.length(), sqlstr_.ptr());
  }
  else if (OB_SUCCESS != (ret = encode_vi64(buf, len, pos, fields.count())))
  {
    TBSYS_LOG(ERROR, "encode fields count error, ret = [%d]", ret);
  }
  else
  {
    for (int i = 0; i < fields.count(); i++)
    {
      if (OB_SUCCESS != fields.at(i).tname_.serialize(buf, len, pos)
          || OB_SUCCESS != fields.at(i).org_tname_.serialize(buf, len, pos)
          || OB_SUCCESS != fields.at(i).cname_.serialize(buf, len, pos)
          || OB_SUCCESS != fields.at(i).org_cname_.serialize(buf, len, pos))
        /* || OB_SUCCESS != fields.at(i).type_.serialize(buf, len, pos)) */
      {
        ret = OB_SERIALIZE_ERROR;
        break;
      }
    }

    const ObRowDesc *row_desc = NULL;
    bool select_stmt = rs_.is_with_rows();
    if (OB_SUCCESS == ret && select_stmt)
    {
      if (OB_SUCCESS != (ret = rs_.get_row_desc(row_desc)))
      {
        TBSYS_LOG(ERROR, "get row desc error, ret = [%d]", ret);
      }
      else if (NULL == row_desc)
      {
        TBSYS_LOG(ERROR, "row or row_desc is NULL");
        ret = OB_SERIALIZE_ERROR;
      }
      else if (OB_SUCCESS != (ret = row_desc->serialize(buf, len, pos)))
      {
        TBSYS_LOG(ERROR, "serilize row desc error, ret = [%d]", ret);
      }
      else if (NULL != extra_row_)
      {
        row = extra_row_;
      }

      if (OB_SUCCESS == ret || OB_ITER_END == ret)
      {
        /* carry data or not */
        scanner_.set_mem_size_limit(
          ObNewScanner::DEFAULT_MAX_SERIALIZE_SIZE - pos);
        do
        {
          if (NULL != row)
          {
            if (OB_SIZE_OVERFLOW == (ret = scanner_.add_row(*row)))
            {
              TBSYS_LOG(DEBUG, "add new row to ob_new_scanner "
                        "fullfilled while execute sql [%.*s] ",
                        sqlstr_.length(), sqlstr_.ptr());
              break;
            }
            else if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "add new row to ob_new_scanner error "
                        "while execute sql [%.*s], ret = [%d]",
                        sqlstr_.length(), sqlstr_.ptr(), ret);
              break;
            }
          }
          else
          {
            /* fisrt loop first serialize */
          }
        } while (OB_SUCCESS == (ret = rs_.get_next_row(row)));
        if (OB_ITER_END == ret)
        {
          /* process successfully and no data remain */
          set_fullfilled(true);
          extra_row_ = NULL;
          ret = scanner_.serialize(buf, len, pos);
        }
        else if (OB_SIZE_OVERFLOW == ret)
        {
          /* process successfully and have more data need serilize */
          set_fullfilled(false);
          extra_row_ = row;
          ret = scanner_.serialize(buf, len, pos);
		  //add peiouya [MULTI_PACKET_BUG_FIX] 20141121:b
		  scanner_.reuse();
		  //add 20141121:e
        }
        else if (OB_SUCCESS != ret) /* else */
        {
          TBSYS_LOG(ERROR, "fail to serialize scanner, ret = [%d]", ret);
        }
      }
      else
      {
        set_fullfilled(true);
      }
    }
  }

  if (OB_SUCCESS != ret)
  {
    pos = pos_bk;
  }
  return ret;
}

int ObSQLResultSet::deserialize(const char *buf,
                                const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t field_cnt = 0;

  clear();
  /* deserialize fields info */
  if (OB_SUCCESS != (ret = decode_vi32(buf, len, pos, &errno_)))
  {
    TBSYS_LOG(ERROR, "deserialize errno error! ret = [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = sqlstr_.deserialize(buf, len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize sqlstr error! ret = [%d]", ret);
  }
  else if (OB_SUCCESS != errno_)
  {
    TBSYS_LOG(WARN, "execute sql [%.*s] error, errno: [%d]",
              sqlstr_.length(), sqlstr_.ptr(), errno_);
  }
  else if (OB_SUCCESS != (ret = decode_vi64(buf, len, pos, &field_cnt)))
  {
    TBSYS_LOG(ERROR, "deserialize fields count error! ret = [%d]", ret);
  }
  else
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < field_cnt; ++i)
    {
      sql::ObResultSet::Field field;
      if (OB_SUCCESS != field.tname_.deserialize(buf, len, pos)
          || OB_SUCCESS != field.org_tname_.deserialize(buf, len, pos)
          || OB_SUCCESS != field.cname_.deserialize(buf, len, pos)
          || OB_SUCCESS != field.org_cname_.deserialize(buf, len, pos))
      {
        ret = OB_DESERIALIZE_ERROR;
        TBSYS_LOG(ERROR, "deserialize fields error, ret = [%d]", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = fields_.push_back(field)))
      {
        TBSYS_LOG(ERROR, "push field into array error, ret = [%d]", ret);
      }
    }

    if (OB_SUCCESS == ret && field_cnt > 0)
    {
      if (OB_SUCCESS != (ret = row_desc_.deserialize(buf, len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize row desc error, ret = [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = scanner_.deserialize(buf, len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize new scanner data error, ret = [%d]", ret);
      }
      else
      {
        scanner_.set_default_row_desc(&row_desc_);
        TBSYS_LOG(DEBUG, "deserialize sql_proxy successfully!");
      }
    }
  }
  return ret;
}
