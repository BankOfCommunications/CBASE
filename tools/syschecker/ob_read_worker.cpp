/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_read_worker.cpp for define read worker thread.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <math.h>
#include <set>
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/murmur_hash.h"
#include "common/ob_action_flag.h"
#include "ob_read_worker.h"

namespace oceanbase
{
  namespace syschecker
  {
    using namespace tbsys;
    using namespace common;
    using namespace serialization;
    using namespace client;

    ObReadWorker::ObReadWorker(ObClient& client, ObSyscheckerRule& rule,
        ObSyscheckerStat& stat, ObSyscheckerParam& param)
      : client_(client), read_rule_(rule), stat_(stat), param_(param)
    {

    }

    ObReadWorker::~ObReadWorker()
    {

    }

    int ObReadWorker::init(ObOpParam** read_param)
    {
      int ret = OB_SUCCESS;

      if (NULL != read_param && NULL == *read_param)
      {
        *read_param = reinterpret_cast<ObOpParam*>(ob_malloc(sizeof(ObOpParam), ObModIds::TEST));
        if (NULL == *read_param)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for read param");
          ret = OB_ERROR;
        }
        else
        {
          memset(*read_param, 0, sizeof(ObOpParam));
        }
      }

      return ret;
    }

    int ObReadWorker::check_cell(const ObOpCellParam& cell_param,
        const ObCellInfo& cell_info)
    {
      int ret         = OB_SUCCESS;
      ObObjType type  = ObNullType;

      if (OB_SUCCESS == ret
          && cell_info.column_name_ != cell_param.column_name_)
      {
        TBSYS_LOG(WARN, "check column name failed, "
            "expected column_name=%.*s, actual column_name=%.*s",
            cell_param.column_name_.length(), cell_param.column_name_.ptr(),
            cell_info.column_name_.length(), cell_info.column_name_.ptr());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret )
      {
        type = cell_info.value_.get_type();
        if (type != cell_param.cell_type_ && type != ObNullType)
        {
          TBSYS_LOG(WARN, "check column type failed, "
              "expected column_type=%s, actual column_type=%s",
              OBJ_TYPE_STR[cell_param.cell_type_], OBJ_TYPE_STR[type]);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObReadWorker::check_null_cell(const int64_t prefix,
        const ObCellInfo& cell_info,
        const ObCellInfo& aux_cell_info)
    {
      int ret                 = OB_SUCCESS;
      int64_t aux_intv        = 0;
      float aux_floatv        = 0.0;
      double aux_doublev      = 0.0;
      ObObjType cell_type     = cell_info.value_.get_type();
      ObObjType aux_cell_type = aux_cell_info.value_.get_type();

      if (ObNullType == cell_type)
      {
        switch (aux_cell_type)
        {
          case ObNullType:
            break;

          case ObIntType:
            aux_cell_info.value_.get_int(aux_intv);
            if (prefix != aux_intv)
            {
              TBSYS_LOG(WARN, "check int value failed, column name=%.*s, "
                  "expected value=%ld, actual value=%ld",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(), prefix, aux_intv);
              ret = OB_ERROR;
            }
            break;

          case ObFloatType:
            aux_cell_info.value_.get_float(aux_floatv);
            if (fabs((float)prefix - aux_floatv) > 0.0001)
            {
              TBSYS_LOG(WARN, "check float value failed, column name=%.*s, "
                  "expected value=%.4f, actual value=%.4f",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(), (float)prefix, aux_floatv);
              ret = OB_ERROR;
            }
            break;

          case ObDoubleType:
            aux_cell_info.value_.get_double(aux_doublev);
            if (fabs((double)prefix - aux_doublev) > 0.0001)
            {
              TBSYS_LOG(WARN, "check double value failed, column name=%.*s, "
                  "expected value=%.4f, actual value=%.4f",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(), (double)prefix, aux_doublev);
              ret = OB_ERROR;
            }
            break;

          default:
            TBSYS_LOG(WARN, "invalid auxiliary cell info type %s",
                OBJ_TYPE_STR[aux_cell_type]);
            break;
        }
      }

      return  ret;
    }

    int ObReadWorker::check_cell(const ObOpCellParam& cell_param,
        const ObOpCellParam& aux_cell_param,
        const ObCellInfo& cell_info,
        const ObCellInfo& aux_cell_info)
    {
      int ret                       = OB_SUCCESS;
      int64_t prefix                = 0;
      int64_t intv                  = 0;
      int64_t aux_intv              = 0;
      float floatv                  = 0.0;
      float aux_floatv              = 0.0;
      double doublev                = 0.0;
      double aux_doublev            = 0.0;
      ObDateTime datetimev          = 0;
      int64_t aux_datetimev         = 0;
      ObPreciseDateTime pdatetimev  = 0;
      int64_t aux_pdatetimev        = 0;
      int64_t aux_varcharv          = 0;
      int64_t varchar_hash          = 0;
      ObCreateTime createtimev      = 0;
      ObCreateTime aux_createtimev  = 0;
      ObModifyTime modifytimev      = 0;
      ObModifyTime aux_modifytimev  = 0;
      ObObjType cell_type           = cell_info.value_.get_type();
      ObObjType aux_cell_type       = aux_cell_info.value_.get_type();
      ObString varcharv;
      struct MurmurHash2 hash;

      ret = check_cell(cell_param, cell_info);
      if (OB_SUCCESS == ret)
      {
        ret = check_cell(aux_cell_param, aux_cell_info);
      }

      /**
       * in order to make test easier, we make prefix 0, it means the
       * auxiliary column store the opposite number of original, in
       * other word, org + aux = 0
       */
      if (OB_SUCCESS == ret)
      {
        switch (cell_type)
        {
          case ObNullType:
            ret = check_null_cell(prefix, cell_info, aux_cell_info);
            break;

          case ObIntType:
            if (aux_cell_type != ObIntType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObIntType, "
                  "but it's %s", OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_int(intv);
            aux_cell_info.value_.get_int(aux_intv);
            if (intv != prefix - aux_intv)
            {
              TBSYS_LOG(WARN, "check int value failed, column name=%.*s, "
                  "expected value=%ld, actual value=%ld, "
                  "prefix=%ld, aux_intv=%ld",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(), prefix - aux_intv, intv,
                  prefix, aux_intv);
              ret = OB_ERROR;
            }
            break;

          case ObFloatType:
            if (aux_cell_type != ObFloatType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObFloatType, "
                  "but it's %s", OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_float(floatv);
            aux_cell_info.value_.get_float(aux_floatv);
            if (fabs(floatv - aux_floatv) > 0.0001)
            {
              cell_info.value_.get_float(floatv);
              aux_cell_info.value_.get_float(aux_floatv);
              TBSYS_LOG(WARN, "check float value failed, column name=%.*s, "
                  "expected value=%f, actual value=%f",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(), aux_floatv, floatv);
              ret = OB_ERROR;
            }
            break;

          case ObDoubleType:
            if (aux_cell_type != ObDoubleType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObDoubleType, "
                  "but it's %s", OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_double(doublev);
            aux_cell_info.value_.get_double(aux_doublev);
            if (fabs(doublev - aux_doublev) > 0.0001)
            {
              TBSYS_LOG(WARN, "check double value failed, column name=%.*s, "
                  "expected value=%.4f, actual value=%.4f",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(), aux_doublev, doublev);
              ret = OB_ERROR;
            }
            break;

          case ObDateTimeType:
            if (aux_cell_type != ObIntType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObIntType, "
                  "but it's %s", OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_datetime(datetimev);
            aux_cell_info.value_.get_int(aux_datetimev);
            if (datetimev != prefix - aux_datetimev)
            {
              TBSYS_LOG(WARN, "check ObDateTime value failed, column name=%.*s, "
                  "expected value=%ld, actual value=%ld, "
                  "prefix=%ld, aux_datetimev=%ld",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(),
                  prefix - aux_datetimev, datetimev,
                  prefix, aux_datetimev);
              ret = OB_ERROR;
            }
            break;

          case ObPreciseDateTimeType:
            /**
             * for the column with ObCreateTimeType or ObModifyTimeType in
             * join table, it's corresponding join column in wide table is
             * with ObPreciseDateTimeType, and the column in wide table has
             * no auxiliary column, when get or scan this column in wide, we
             * will get the column twice
             */
            if (aux_cell_type != ObIntType
                && aux_cell_type != ObPreciseDateTimeType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObIntType or "
                  "ObPreciseDateTimeType, but it's %s",
                  OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_precise_datetime(pdatetimev);
            aux_cell_info.value_.get_int(aux_pdatetimev);
            if ((aux_cell_type == ObIntType
                  && datetimev != prefix - aux_pdatetimev)
                || (aux_cell_type == ObPreciseDateTimeType
                  && datetimev != aux_pdatetimev))
            {
              TBSYS_LOG(WARN, "check ObPreciseDateTime value failed, "
                  "column name=%.*s, expected value=%ld, "
                  "actual value=%ld, prefix=%ld, aux_pdatetimev=%ld",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(),
                  prefix - aux_pdatetimev, pdatetimev,
                  prefix, aux_pdatetimev);
              ret = OB_ERROR;
            }
            break;

          case ObVarcharType:
            if (aux_cell_type != ObIntType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObIntType, "
                  "but it's %s, column_name=%.*s",
                  OBJ_TYPE_STR[aux_cell_type],
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr());
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_varchar(varcharv);
            aux_cell_info.value_.get_int(aux_varcharv);
            varchar_hash = hash(varcharv.ptr(), varcharv.length());
            if (varchar_hash != prefix - aux_varcharv)
            {
              TBSYS_LOG(WARN, "check ObVarchar value failed, table=%ld, rowkey=%s, column name=%.*s, "
                  "actual hash value=%ld,"
                  "origin varchar=%.*s, prefix=%ld, aux_varcharv=%ld",
                  cell_info.table_id_,
                  to_cstring(cell_info.row_key_),
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(),
                  varchar_hash,
                  varcharv.length(), varcharv.ptr(), prefix, aux_varcharv);
              ret = OB_ERROR;
            }
            break;

          case ObCreateTimeType:
            if (aux_cell_type != ObCreateTimeType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObIntType, "
                  "but it's %s", OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_createtime(createtimev);
            aux_cell_info.value_.get_createtime(aux_createtimev);
            if (createtimev != aux_createtimev)
            {
              TBSYS_LOG(WARN, "check createtime value failed, column name=%.*s, "
                  "expected value=%ld, actual value=%ld, ",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(),
                  aux_createtimev, createtimev);
              ret = OB_ERROR;
            }
            break;

          case ObModifyTimeType:
            if (aux_cell_type != ObModifyTimeType)
            {
              TBSYS_LOG(WARN, "expect auxiliary cell type is ObIntType, "
                  "but it's %s", OBJ_TYPE_STR[aux_cell_type]);
              ret = OB_ERROR;
              break;
            }
            cell_info.value_.get_modifytime(modifytimev);
            aux_cell_info.value_.get_modifytime(aux_modifytimev);
            if (modifytimev != aux_modifytimev)
            {
              TBSYS_LOG(WARN, "check modifytime value failed, column name=%.*s, "
                  "expected value=%ld, actual value=%ld, ",
                  cell_info.column_name_.length(),
                  cell_info.column_name_.ptr(),
                  aux_modifytimev, modifytimev);
              ret = OB_ERROR;
            }
            break;

          case ObSeqType:
          case ObExtendType:
          default:
            TBSYS_LOG(WARN, "invalid cell info type %s",
                OBJ_TYPE_STR[cell_type]);
            ret = OB_ERROR;
            break;
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "%s", to_cstring(cell_info.row_key_));
      }

      //ret = OB_SUCCESS; //for debug
      return ret;
    }

    int ObReadWorker::display_scanner(const ObScanner& scanner) const
    {
      int ret                       = OB_SUCCESS;
      ObCellInfo* cell_info         = NULL;
      bool is_row_changed           = false;
      int64_t row_index             = 0;
      int64_t column_index          = 0;
      ObObjType type                = ObMinType;
      int64_t ext_type              = 0;
      int64_t intv                  = 0;
      float floatv                  = 0.0;
      double doublev                = 0.0;
      ObDateTime datetimev          = 0;
      ObPreciseDateTime pdatetimev  = 0;
      ObCreateTime createtimev      = 0;
      ObModifyTime modifytimev      = 0;
      ObString varcharv;
      ObScannerIterator iter;

      for (iter = scanner.begin();
          iter != scanner.end() && OB_SUCCESS == ret; iter++)
      {
        iter.get_cell(&cell_info, &is_row_changed);
        if (NULL == cell_info)
        {
          TBSYS_LOG(WARN, "get null cell_info");
          ret = OB_ERROR;
          break;
        }
        else if (is_row_changed)
        {
          column_index = 0;
          fprintf(stderr, "row[%ld]: ", row_index++);
          fprintf(stderr, "%s", to_cstring(cell_info->row_key_));
        }

        fprintf(stderr, "  cell[%ld]: \n"
            "    column_name=%.*s \n",
            column_index++, cell_info->column_name_.length(),
            cell_info->column_name_.ptr());
        type = cell_info->value_.get_type();
        switch (type)
        {
          case ObNullType:
            fprintf(stderr, "    type=%s \n"
                "    value=NULL \n", OBJ_TYPE_STR[type]);
            break;
          case ObIntType:
            cell_info->value_.get_int(intv);
            fprintf(stderr, "    type=%s \n"
                "    value=%ld \n", OBJ_TYPE_STR[type], intv);
            break;
          case ObFloatType:
            cell_info->value_.get_float(floatv);
            fprintf(stderr, "    type=%s \n"
                "    value=%.4f \n", OBJ_TYPE_STR[type], floatv);
            break;
          case ObDoubleType:
            cell_info->value_.get_double(doublev);
            fprintf(stderr, "    type=%s \n"
                "    value=%.4f \n", OBJ_TYPE_STR[type], doublev);
            break;
          case ObDateTimeType:
            cell_info->value_.get_datetime(datetimev);
            fprintf(stderr, "    type=%s \n"
                "    value=%ld \n", OBJ_TYPE_STR[type], datetimev);
            break;
          case ObPreciseDateTimeType:
            cell_info->value_.get_precise_datetime(pdatetimev);
            fprintf(stderr, "    type=%s \n"
                "    value=%ld \n", OBJ_TYPE_STR[type], pdatetimev);
            break;
          case ObVarcharType:
            cell_info->value_.get_varchar(varcharv);
            fprintf(stderr, "    type=%s \n"
                "    value=%p \n", OBJ_TYPE_STR[type], varcharv.ptr());
            break;

          case ObCreateTimeType:
            cell_info->value_.get_createtime(createtimev);
            fprintf(stderr, "    type=%s \n"
                "    value=%ld \n", OBJ_TYPE_STR[type], createtimev);
            break;
          case ObModifyTimeType:
            cell_info->value_.get_modifytime(modifytimev);
            fprintf(stderr, "    type=%s \n"
                "    value=%ld \n", OBJ_TYPE_STR[type], modifytimev);
            break;
          case ObExtendType:
            cell_info->value_.get_ext(ext_type);
            fprintf(stderr, "    type=%s \n"
                "    ext_type=%ld \n", OBJ_TYPE_STR[type], ext_type);
            break;
          case ObSeqType:
          case ObMaxType:
          case ObMinType:
            fprintf(stderr, "    type=%s \n", OBJ_TYPE_STR[type]);
            break;
          default:
            TBSYS_LOG(WARN, "unknown object type %s",
                OBJ_TYPE_STR[type]);
            break;
        }
      }

      return ret;
    }

    int ObReadWorker::check_scanner_result(const ObOpParam& read_param,
        const ObScanner& scanner,
        const bool is_get)
    {
      int ret                   = OB_SUCCESS;
      ObCellInfo* pcell         = NULL;
      ObCellInfo* paux_cell     = NULL;
      bool is_row_changed       = false;
      bool read_aux_cell        = false;
      bool row_non_existent     = false;
      int64_t row_count         = 0;
      int64_t column_index      = 0;
      int64_t total_cell_count  = 0;
      int64_t read_cell_fail    = 0;
      const ObOpRowParam* row_param       = NULL;
      const ObOpCellParam* cell_param     = NULL;
      const ObOpCellParam* aux_cell_param = NULL;
      ObScannerIterator iter;
      ObRowkey row_key;
      ObCellInfo cell_info;

      for (iter = scanner.begin();
          iter != scanner.end() && OB_SUCCESS == ret; iter++)
      {
        if (!read_aux_cell)
        {
          //read original cell
          iter.get_cell(&pcell, &is_row_changed);
          if (NULL == pcell)
          {
            TBSYS_LOG(WARN, "get null cell_info");
            ret = OB_ERROR;
            break;
          }
          else if (is_row_changed)
          {
            column_index = 0;
            row_non_existent = false;
            if (OP_GET == read_param.op_type_)
            {
              row_param = &read_param.row_[row_count++];
              row_key.assign(const_cast<ObObj*>(row_param->rowkey_),
                  row_param->rowkey_len_);

              if (row_key != pcell->row_key_)
              {
                TBSYS_LOG(WARN, "row key isn't consistent");
                TBSYS_LOG(WARN, "%s", to_cstring(row_key));
                TBSYS_LOG(WARN, "%s", to_cstring(pcell->row_key_));
                ret = OB_ERROR;
                break;
              }
            }
            else
            {
              //scan opeation only the first row store cell info
              row_param = &read_param.row_[0];
              row_count++;
            }
          }
          else if (row_non_existent)
          {
            TBSYS_LOG(WARN, "row doesn't exist, but we get other column "
                "in this row, column_name=%.*s, type=%s, ext_type=%ld",
                pcell->column_name_.length(), pcell->column_name_.ptr(),
                OBJ_TYPE_STR[pcell->value_.get_type()],
                pcell->value_.get_ext());
            TBSYS_LOG(WARN, "%s", to_cstring(pcell->row_key_));
            ret = OB_ERROR;
            break;  //ignore all the illegal column
          }

          /**
           * row doesn't exist, merge server only returns one cell to
           * show this row doesn't exist, all the cells in this row
           * doesn't exist, but we may get several cells in this row.
           */
          if (ObExtendType == pcell->value_.get_type()
              && pcell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
          {
            if (OP_SCAN == read_param.op_type_)
            {
              TBSYS_LOG(WARN, "scan operation return 'OP_ROW_DOES_NOT_EXIST' cell, "
                  "column_name=%.*s, type=%s, ext_type=%ld",
                  pcell->column_name_.length(), pcell->column_name_.ptr(),
                  OBJ_TYPE_STR[pcell->value_.get_type()],
                  pcell->value_.get_ext());
              TBSYS_LOG(WARN, "%s", to_cstring(pcell->row_key_));
              ret = OB_ERROR;
              break;
            }
            total_cell_count += row_param->cell_count_;
            row_non_existent = true;
            continue;
          }
          else
          {
            total_cell_count++; //only caculate the original cell
          }

          if (column_index >= row_param->cell_count_)
          {
            TBSYS_LOG(WARN, "column index is greater than row cell count, "
                "column_index=%ld, row_cell_count=%d",
                column_index, row_param->cell_count_);
            ret = OB_ERROR;
            break;
          }
          cell_param = &row_param->cell_[column_index++];
          cell_info = *pcell;
          read_aux_cell = true;
        }
        else
        {
          iter.get_cell(&paux_cell, &is_row_changed);
          if (NULL == paux_cell || is_row_changed)
          {
            TBSYS_LOG(WARN, "get null aux_cell_info or row change:%p,%d, %s",
                paux_cell, is_row_changed, print_cellinfo(paux_cell));
            ret = OB_ERROR;
            break;
          }

          if (column_index >= row_param->cell_count_)
          {
            TBSYS_LOG(WARN, "column index is greater than row cell count, "
                "column_index=%ld, row_cell_count=%d",
                column_index, row_param->cell_count_);
            ret = OB_ERROR;
            break;
          }

          aux_cell_param = &row_param->cell_[column_index++];
          ret = check_cell(*cell_param, *aux_cell_param,
              cell_info, *paux_cell);
          read_aux_cell = false;
          if (OB_SUCCESS != ret)
          {
            read_cell_fail++;
          }
          total_cell_count++;
        }

      }

      if (OB_SUCCESS != ret)
      {
        display_scanner(scanner);
      }

      if (OB_SUCCESS == ret)
      {
        stat_.add_read_cell(total_cell_count);
        stat_.add_read_cell_fail(read_cell_fail);
        if (is_get)
        {
          stat_.add_get_opt(1);
          stat_.add_get_cell(total_cell_count);
          stat_.add_get_cell_fail(read_cell_fail);
        }
        else
        {
          stat_.add_scan_opt(1);
          stat_.add_scan_cell(total_cell_count);
          stat_.add_scan_cell_fail(read_cell_fail);
        }
      }

      return ret;
    }


    int ObReadWorker::run_get(ObOpParam& read_param, ObGetParam& get_param,
        ObScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      ObOpRowParam* row_param = NULL;
      ObCellInfo cell_info;
      ObVersionRange ver_range;
      int64_t start_time = 0;
      int64_t consume_time = 0;

      get_param.reset();
      scanner.clear();
      get_param.set_is_result_cached(false);

      //build get_param
      for (int64_t i = 0; i < read_param.row_count_ && OB_SUCCESS == ret; ++i)
      {
        row_param = &read_param.row_[i];
        for (int64_t j = 0; j < row_param->cell_count_ && OB_SUCCESS == ret; ++j)
        {
          cell_info.reset();
          cell_info.table_name_ = read_param.table_name_;
          cell_info.row_key_.assign(row_param->rowkey_, row_param->rowkey_len_);
          cell_info.column_name_ = row_param->cell_[j].column_name_;
          ret = get_param.add_cell(cell_info);
          if (OB_SUCCESS != ret)
          {
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ver_range.start_version_ = 0;
        ver_range.border_flag_.set_inclusive_start();
        ver_range.border_flag_.set_max_value();
        get_param.set_version_range(ver_range);

        start_time = tbsys::CTimeUtil::getTime();
        ret = client_.ms_get(get_param, scanner);
        consume_time = tbsys::CTimeUtil::getTime() - start_time;
        stat_.record_resp_event(CMD_GET, consume_time, (OB_SUCCESS != ret));
      }

      //check the get result
      if (OB_SUCCESS == ret && param_.is_check_result())
      {
        ret = check_scanner_result(read_param, scanner, true);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get error, ret=%d, \nget_param:%s", ret, to_cstring(get_param));
          dump_scanner(scanner, TBSYS_LOG_LEVEL_WARN);
        }
      }
      else if (OB_DATA_NOT_SERVE == ret || OB_RESPONSE_TIME_OUT == ret)
      {
        //read a key in hole of key range or timeout,skip it
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "get error, ret=%d, \nget_param:%s", ret, to_cstring(get_param));
      }

      return ret;
    }

    int ObReadWorker::run_scan(ObOpParam& read_param, ObScanParam& scan_param,
        ObScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      ObOpRowParam* row_param = NULL;
      ObNewRange range;
      ObVersionRange ver_range;
      int64_t start_time = 0;
      int64_t consume_time = 0;

      scan_param.reset();
      scanner.clear();

      if (2 != read_param.row_count_)
      {
        TBSYS_LOG(WARN, "wrong row count for scan, expect 2 row, but row_count=%ld",
            read_param.row_count_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < read_param.row_count_ && OB_SUCCESS == ret; ++i)
        {
          row_param = &read_param.row_[i];
          if (0 == i)
          {
            for (int64_t j = 0; j < row_param->cell_count_ && OB_SUCCESS == ret; ++j)
            {
              scan_param.add_column(row_param->cell_[j].column_name_);
            }
            range.start_key_.assign(row_param->rowkey_, row_param->rowkey_len_);
            range.border_flag_.set_inclusive_start();
          }
          else
          {
            range.end_key_.assign(row_param->rowkey_, row_param->rowkey_len_);
            range.border_flag_.set_inclusive_end();
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ver_range.start_version_ = 0;
        ver_range.border_flag_.set_inclusive_start();
        ver_range.border_flag_.set_max_value();
        scan_param.set_version_range(ver_range);
        scan_param.set(OB_INVALID_ID, read_param.table_name_, range);
        scan_param.set_scan_size(OB_MAX_PACKET_LENGTH);
        scan_param.set_is_result_cached(false);

        start_time = tbsys::CTimeUtil::getTime();
        ret = client_.ms_scan(scan_param, scanner);
        consume_time = tbsys::CTimeUtil::getTime() - start_time;
        stat_.record_resp_event(CMD_SCAN, consume_time, (OB_SUCCESS != ret));
      }

      //check the scan result
      if (OB_SUCCESS == ret && param_.is_check_result())
      {
        ret = check_scanner_result(read_param, scanner, false);
      }
      else if (OB_DATA_NOT_SERVE == ret || OB_RESPONSE_TIME_OUT == ret)
      {
        //read a key in hole of key range or timeout,skip it
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "scan err=%d dump content of scan param:", ret);
        scan_param.dump();
      }

      return ret;
    }

    void ObReadWorker::run(CThread* thread, void* arg)
    {
      int err               = OB_SUCCESS;
      ObOpParam* read_param = NULL;
      ObGetParam get_param;
      ObScanParam scan_param;
      ObScanner scanner;
      ObSqlScanParam sql_scan_param;
      ObSqlGetParam sql_get_param;
      ObNewScanner new_scanner;
      UNUSED(thread);
      UNUSED(arg);

      if (OB_SUCCESS == init(&read_param) && NULL != read_param)
      {
        while (!_stop)
        {
          err = read_rule_.get_next_read_param(*read_param);
          if (OB_SUCCESS == err)
          {
            if (read_param->row_count_ <= 0)
            {
              continue;
            }

            switch (read_param->op_type_)
            {
              case OP_GET:
                err = run_get(*read_param, get_param, scanner);
                break;
              case OP_SQL_GET:
                err = run_get(*read_param, sql_get_param, new_scanner);
                break;
              case OP_SCAN:
                err = run_scan(*read_param, scan_param, scanner);
                break;
              case OP_SQL_SCAN:
                err = run_scan(*read_param, sql_scan_param, new_scanner);
                break;
              default:
                TBSYS_LOG(WARN, "wrong op type :%d", read_param->op_type_);
                err = OB_ERROR;
            }
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to run read operation, err=%d", err);
              read_param->display();
              read_param->write_param_to_file(READ_PARAM_FILE);
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "get next read param failed");
          }
        }
      }

      if (NULL != read_param)
      {
        ob_free(read_param);
        read_param = NULL;
      }
    }

    int ObReadWorker::fill_project(int64_t table_id, int64_t column_id, sql::ObProject& project)
    {
      int ret = OB_SUCCESS;
      sql::ObSqlExpression sql_expression;
      sql::ExprItem item;

      item.value_.cell_.tid = table_id;
      item.value_.cell_.cid = column_id;
      item.type_ = T_REF_COLUMN;
      sql_expression.set_tid_cid(table_id, column_id);

      if (OB_SUCCESS != (ret = sql_expression.add_expr_item(item)))
      {
        TBSYS_LOG(WARN, "add_expr_item ret=%d, tid=%ld, cid=%ld", ret, table_id, column_id);
      }
      else if (OB_SUCCESS != (ret = sql_expression.add_expr_item_end()))
      {
        TBSYS_LOG(WARN, "add_expr_item_end ret=%d, tid=%ld, cid=%ld", ret, table_id, column_id);
      }
      else if (OB_SUCCESS != (ret = project.add_output_column(sql_expression)))
      {
        TBSYS_LOG(WARN, "add_output_column ret=%d, tid=%ld, cid=%ld", ret, table_id, column_id);
      }
      return ret;
    }

    int ObReadWorker::fill_rowkey_project(const int64_t table_id, sql::ObProject &project, common::ObRowDesc& row_desc)
    {
      int ret = OB_SUCCESS;
      const ObRowkeyInfo& rowkey_info = read_rule_.get_schema().get_rowkey_info(table_id);
      row_desc.set_rowkey_cell_count(rowkey_info.get_size());
      uint64_t column_id = 0;
      // fill rowkey columns first;
      for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; ++i)
      {
        column_id = rowkey_info.get_column(i)->column_id_;
        if (OB_SUCCESS != (ret = fill_project(table_id, column_id, project)))
        {
          TBSYS_LOG(ERROR, "fill_project tid:[%ld],cid:[%ld]", table_id, column_id);
        }
        else if (OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, column_id)))
        {
          TBSYS_LOG(ERROR, "add_column_desc tid:[%ld],cid:[%ld]", table_id, column_id);
        }
      }
      return ret;
    }

    int ObReadWorker::fill_sql_scan_param(ObOpParam& read_param, sql::ObSqlScanParam& scan_param, common::ObRowDesc& row_desc)
    {
      int ret = OB_SUCCESS;
      ObNewRange range;
      ObProject project;
      ObOpRowParam* row_param = NULL;
      int64_t last_frozen_version = 0;
      std::set<uint64_t> column_id_set;
      uint64_t column_id = 0;

      scan_param.reset();

      if (2 != read_param.row_count_)
      {
        TBSYS_LOG(WARN, "wrong row count for scan, expect 2 row, but row_count=%ld",
            read_param.row_count_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = client_.get_last_frozen_version(last_frozen_version)))
      {
        TBSYS_LOG(WARN, "get_last_frozen_version ret=%d", ret);
      }
      else
      {
        fill_rowkey_project(read_param.table_id_, project, row_desc);
        for (int64_t i = 0; i < read_param.row_count_ && OB_SUCCESS == ret; ++i)
        {
          row_param = &read_param.row_[i];
          if (0 == i)
          {
            for (int64_t j = 0; j < row_param->cell_count_ && OB_SUCCESS == ret; ++j)
            {
              column_id = row_param->cell_[j].column_id_;
              if (column_id_set.find(column_id) == column_id_set.end())
              {
                fill_project(read_param.table_id_, column_id, project);
                row_desc.add_column_desc(read_param.table_id_, column_id);
                column_id_set.insert(column_id);
              }
            }
            range.start_key_.assign(row_param->rowkey_, row_param->rowkey_len_);
            range.border_flag_.set_inclusive_start();
          }
          else
          {
            range.end_key_.assign(row_param->rowkey_, row_param->rowkey_len_);
            range.border_flag_.set_inclusive_end();
          }
        }
        range.table_id_ = read_param.table_id_;
      }


      if (OB_SUCCESS == ret)
      {
        //TBSYS_LOG(INFO, "scan range=%s, project=%s", to_cstring(range), to_cstring(project));
        scan_param.set_table_id(read_param.table_id_, read_param.table_id_);
        scan_param.set_data_version(last_frozen_version);
        scan_param.set_range(range);
        scan_param.set_is_result_cached(false);
        scan_param.set_project(project);
      }
      return ret;
    }

    int ObReadWorker::fill_sql_get_param(ObOpParam& read_param, sql::ObSqlGetParam& get_param, common::ObRowDesc& row_desc)
    {
      int ret = OB_SUCCESS;
      ObProject project;
      ObOpRowParam* row_param = NULL;
      int64_t last_frozen_version = 0;
      ObRowkey rowkey;

      get_param.reset();

      if (OB_SUCCESS != (ret = client_.get_last_frozen_version(last_frozen_version)))
      {
        TBSYS_LOG(WARN, "get_last_frozen_version ret=%d", ret);
      }
      else
      {
        fill_rowkey_project(read_param.table_id_, project, row_desc);
        for (int64_t i = 0; i < read_param.row_count_ && OB_SUCCESS == ret; ++i)
        {
          row_param = &read_param.row_[i];
          rowkey.assign(row_param->rowkey_, row_param->rowkey_len_);
          get_param.add_rowkey(rowkey, false);
          if (i == 0)
          {
            ret = fill_row_project(read_param.table_id_, *row_param, project, row_desc);
          }
        }
      }


      if (OB_SUCCESS == ret)
      {
        get_param.set_table_id(read_param.table_id_, read_param.table_id_);
        get_param.set_data_version(last_frozen_version);
        get_param.set_is_result_cached(false);
        get_param.set_project(project);
      }
      //TBSYS_LOG(INFO, "get_param tid=%ld, rename tid=%ld", get_param.get_table_id(), get_param.get_renamed_table_id());
      return ret;
    }

    int ObReadWorker::fill_row_project(const int64_t table_id, const ObOpRowParam& row_param,
        sql::ObProject &project, common::ObRowDesc& row_desc)
    {
      int ret = OB_SUCCESS;
      std::set<uint64_t> column_id_set;
      int64_t column_id = 0;
      for (int64_t j = 0; j < row_param.cell_count_ && OB_SUCCESS == ret; ++j)
      {
        column_id = row_param.cell_[j].column_id_;
        if (column_id_set.find(column_id) == column_id_set.end())
        {
          fill_project(table_id, column_id, project);
          row_desc.add_column_desc(table_id, column_id);
          column_id_set.insert(column_id);
        }
      }
      return ret;
    }

    int ObReadWorker::run_scan(ObOpParam& read_param, ObSqlScanParam& scan_param, ObNewScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      int64_t start_time = 0;
      int64_t consume_time = 0;

      scanner.clear();
      ObRowDesc row_desc;

      if (2 != read_param.row_count_)
      {
        TBSYS_LOG(WARN, "wrong row count for scan, expect 2 row, but row_count=%ld",
            read_param.row_count_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = fill_sql_scan_param(read_param, scan_param, row_desc);
      }

      if (OB_SUCCESS == ret)
      {
        start_time = tbsys::CTimeUtil::getTime();
        ret = client_.cs_sql_scan(scan_param, scanner);
        consume_time = tbsys::CTimeUtil::getTime() - start_time;
        stat_.record_resp_event(CMD_SCAN, consume_time, (OB_SUCCESS != ret));
      }

      //check the scan result
      if (OB_SUCCESS == ret && param_.is_check_result())
      {
        ret = check_scanner_result(read_param, row_desc, scanner, false);
      }
      else if (OB_DATA_NOT_SERVE == ret || OB_RESPONSE_TIME_OUT == ret)
      {
        //read a key in hole of key range or timeout,skip it
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "scan err=%d dump content of scan param:", ret);
        scan_param.dump();
      }

      return ret;
    }

    int ObReadWorker::run_get(ObOpParam& read_param, ObSqlGetParam& get_param, ObNewScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      ObCellInfo cell_info;
      int64_t start_time = 0;
      int64_t consume_time = 0;

      get_param.reset();
      scanner.clear();
      ObRowDesc row_desc;

      //build get_param
      if (OB_SUCCESS == ret)
      {
        ret = fill_sql_get_param(read_param, get_param, row_desc);
      }

      if (OB_SUCCESS == ret)
      {
        start_time = tbsys::CTimeUtil::getTime();
        ret = client_.cs_sql_get(get_param, scanner);
        consume_time = tbsys::CTimeUtil::getTime() - start_time;
        stat_.record_resp_event(CMD_SCAN, consume_time, (OB_SUCCESS != ret));
      }

      //check the scan result
      if (OB_SUCCESS == ret && param_.is_check_result())
      {
        //ret = check_scanner_result(read_param, row_desc, scanner, false);
      }
      else if (OB_DATA_NOT_SERVE == ret || OB_RESPONSE_TIME_OUT == ret)
      {
        //read a key in hole of key range or timeout,skip it
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "scan err=%d dump content of scan param:", ret);
      }

      return ret;
    }

    int ObReadWorker::check_scanner_result(const ObOpParam& read_param,
        const ObRowDesc& row_desc,
        const ObNewScanner& scanner,
        const bool is_get)
    {
      int ret                   = OB_SUCCESS;
      int64_t prefix            = 0;
      int64_t row_count         = 0;
      int64_t column_index      = 0;
      int64_t total_cell_count  = 0;
      int64_t read_cell_fail    = 0;
      ObObj* pcell              = NULL;
      ObObj* aux_pcell          = NULL;
      const ObRowkey *row_key   = NULL;
      const ObOpRowParam* row_param       = NULL;
      const ObOpCellParam* cell_param     = NULL;
      const ObOpCellParam* aux_cell_param = NULL;

      ObCellInfo cell_info;
      ObCellInfo aux_cell_info;
      common::ObRow current_row;

      current_row.set_row_desc(row_desc);
      while (OB_SUCCESS == ret &&
          OB_SUCCESS == const_cast<ObNewScanner&>(scanner).get_next_row(current_row))
      {
        if (OP_GET == read_param.op_type_)
        {
          row_param = &read_param.row_[row_count++];
        }
        else
        {
          //scan opeation only the first row store cell info
          row_param = &read_param.row_[0];
          row_count++;
        }

        if (OB_SUCCESS != (ret = current_row.get_rowkey(row_key))
            || NULL == row_key || row_key->get_obj_cnt() <= 0)
        {
          TBSYS_LOG(ERROR, "rowkey not correct=%d", ret);
        }
        else if (row_key->get_obj_ptr()[0].get_type() != ObIntType)
        {
          TBSYS_LOG(ERROR, "rowkey(%s) prefix not int type", to_cstring(*row_key));
        }
        else if (OB_SUCCESS != (ret = row_key->get_obj_ptr()[0].get_int(prefix)))
        {
          TBSYS_LOG(ERROR, "rowkey(%s) prefix error,ret=%d", to_cstring(*row_key), ret);
        }
        else
        {
          for (column_index = 0; column_index < row_param->cell_count_ && OB_SUCCESS == ret; column_index += MIN_OP_CELL_COUNT)
          {
            cell_param = &row_param->cell_[column_index];
            aux_cell_param = &row_param->cell_[column_index + 1];
            if (OB_SUCCESS != (ret = current_row.get_cell(read_param.table_id_, cell_param->column_id_, pcell)))
            {
              TBSYS_LOG(ERROR, "get org cell ,tid=[%ld], cid=[%ld], ret=%d",
                  read_param.table_id_, cell_param->column_id_, ret);
            }
            else if (OB_SUCCESS != (ret = current_row.get_cell(read_param.table_id_, aux_cell_param->column_id_, aux_pcell)))
            {
              TBSYS_LOG(ERROR, "get aux cell ,tid=[%ld], cid=[%ld], ret=%d",
                  read_param.table_id_, cell_param->column_id_, ret);
            }
            else
            {
              cell_info.table_id_ = read_param.table_id_;
              cell_info.column_name_ = cell_param->column_name_;
              cell_info.column_id_ = cell_param->column_id_;
              cell_info.value_ = *pcell;

              aux_cell_info.table_id_ = read_param.table_id_;
              aux_cell_info.column_name_ = aux_cell_param->column_name_;
              aux_cell_info.column_id_ = aux_cell_param->column_id_;
              aux_cell_info.value_ = *aux_pcell;

              if (OB_SUCCESS != (ret = check_cell(*cell_param, *aux_cell_param, cell_info, aux_cell_info)))
              {
                TBSYS_LOG(ERROR, "check_cell error, prefix=%ld, cid=%ld, cell=%s",
                    prefix, cell_param->column_id_, to_cstring(*pcell));
              }
            }


            /*
            TBSYS_LOG(INFO, "check_scanner_result, rowkey:%s,idx:%ld,tid:%ld,cid:%ld, pcell:%s",
                to_cstring(*row_key), column_index, read_param.table_id_, cell_param->column_id_, to_cstring(*pcell));
            */
          }
        }

        total_cell_count += row_param->cell_count_;

      }

      if (OB_SUCCESS == ret)
      {
        stat_.add_read_cell(total_cell_count);
        stat_.add_read_cell_fail(read_cell_fail);
        if (is_get)
        {
          stat_.add_get_opt(1);
          stat_.add_get_cell(total_cell_count);
          stat_.add_get_cell_fail(read_cell_fail);
        }
        else
        {
          stat_.add_scan_opt(1);
          stat_.add_scan_cell(total_cell_count);
          stat_.add_scan_cell_fail(read_cell_fail);
        }
      }

      return ret;
    }

  } // end namespace syschecker
} // end namespace oceanbase
