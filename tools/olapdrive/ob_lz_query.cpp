/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_lz_query.cpp for define abstract lz query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include<algorithm>
#include <math.h>
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/murmur_hash.h"
#include "common/ob_action_flag.h"
#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace serialization;
    using namespace client;

    const char* OBJ_TYPE_STR[] =
    {
      "ObNullType",
      "ObIntType",
      "ObFloatType",
      "ObDoubleType",
      "ObDateTimeType",
      "ObPreciseDateTimeType",
      "ObVarcharType",
      "ObSeqType",
      "ObCreateTimeType",
      "ObModifyTimeType",
      "ObExtendType",
      "ObMaxType"
    };

    ObLzQuery::ObLzQuery(ObClient& client, ObOlapdriveMeta& meta,
                         ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                         ObOlapdriveStat& stat)
    : client_(client), meta_(meta), schema_(schema), param_(param),
      stat_(stat), rowkey_meta_(NULL), log_level_(0)
    {

    }

    ObLzQuery::~ObLzQuery()
    {

    }

    int ObLzQuery::init()
    {
      int ret = OB_SUCCESS;

      rowkey_meta_ = &meta_.get_rowkey_meta();
      log_level_ = param_.get_log_level();

      return ret;
    }

    int64_t ObLzQuery::tv_to_microseconds(const timeval & tp)
    {
      return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
    }

    int64_t ObLzQuery::get_cur_microseconds_time()
    {
      struct timeval tp;
      gettimeofday(&tp, NULL);
      return tv_to_microseconds(tp);
    }

    int ObLzQuery::display_scanner(const ObScanner& scanner) const
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
          fprintf(stderr, "row[%ld]: %s", row_index++, to_cstring(cell_info->row_key_));
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

    int64_t ObLzQuery::get_random_unit_id()
    {
      return (random() % rowkey_meta_->max_unit_id_);
    }

    int64_t ObLzQuery::get_random_start_date(int64_t& day_count)
    {
      int64_t start_date = 0;
      int64_t date_cnt = rowkey_meta_->max_date_ - rowkey_meta_->min_date_;

      if (day_count <= 0)
      {
        start_date = 0;
        day_count = 0;
      }
      else if (date_cnt <= day_count)
      {
        start_date = rowkey_meta_->min_date_;
        day_count = date_cnt;
      }
      else if (date_cnt > day_count)
      {
        start_date = rowkey_meta_->min_date_ + random() % (date_cnt - day_count);
      }

      return start_date;
    }

    int64_t ObLzQuery::get_random_adgroup_id()
    {
      int err             = OB_SUCCESS;
      int64_t adgroup_id  = 0;
      ObCampaign campaign;

      err = get_cur_scan_campaign(campaign);
      if (OB_SUCCESS == err)
      {
        adgroup_id = campaign.start_adgroup_id_ 
          + random() % (campaign.end_adgroup_id_ - campaign.start_adgroup_id_);
      }
      else
      {
        TBSYS_LOG(WARN, "failed to get current scan campaign, err=%d", err);
      }

      return adgroup_id;
    }

    bool ObLzQuery::is_val_existent(
      int64_t* array, const int64_t item_cnt, const int64_t value)
    {
      bool bret = false;

      if (NULL == array || item_cnt < 0)
      {
        TBSYS_LOG(WARN, "invalid param, array=%p, item_cnt=%ld",
                  array, item_cnt);
      }
      else
      {
        for (int64_t i = 0; i < item_cnt; ++i)
        {
          if (array[i] == value)
          {
            bret = true;
            break;
          }
        }
      }

      return bret;
    }

    int ObLzQuery::get_random_adgroup_id_array(
      const ObCampaign& campaign, 
      int64_t* adgroup_id_array, 
      int64_t& adgroup_cnt)
    {
      int ret                 = OB_SUCCESS;
      int64_t adgroup_id      = 0;
      int64_t act_adgroup_cnt = 0;
     
      if (NULL == adgroup_id_array || adgroup_cnt < 0)
      {
        TBSYS_LOG(WARN, "invalid param, adgroup_id_array=%p, adgroup_cnt=%ld",
                  adgroup_id_array, adgroup_cnt);
        ret = OB_ERROR;
      }
      else
      {
        for (int64_t i = 0; act_adgroup_cnt < adgroup_cnt && i < 100; ++i)
        {
          adgroup_id = campaign.start_adgroup_id_ 
            + random() % (campaign.end_adgroup_id_ - campaign.start_adgroup_id_);
          if (!is_val_existent(adgroup_id_array, act_adgroup_cnt, adgroup_id))
          {
            adgroup_id_array[act_adgroup_cnt++] = adgroup_id;
          }
        }
        adgroup_cnt = act_adgroup_cnt;

        std::sort(adgroup_id_array, adgroup_id_array + adgroup_cnt);
      }

      return ret;
    }

    int ObLzQuery::get_random_bidword_id_array(
      const ObCampaign& campaign, 
      const int64_t* adgroup_id_array, 
      const int64_t adgroup_cnt,
      int64_t* bidword_id_array, 
      int64_t& bidword_cnt)
    {
      int ret                 = OB_SUCCESS;
      int64_t act_bidword_cnt = 0;
      ObAdgroup adgroup;

      if (NULL == adgroup_id_array || adgroup_cnt < 0 
          || NULL == bidword_id_array || bidword_cnt < 0)
      {
        TBSYS_LOG(WARN, "invalid param, adgroup_id_array=%p, adgroup_cnt=%ld, "
                        "bidword_id_array=%p, bidword_cnt=%ld",
                  adgroup_id_array, adgroup_cnt, bidword_id_array, bidword_cnt);
        ret = OB_ERROR;
      }

      for (int64_t i = 0; i < adgroup_cnt && OB_SUCCESS == ret; ++i)
      {
        ret = meta_.get_adgroup(campaign.campaign_id_, campaign.campaign_id_, 
                                adgroup_id_array[i], adgroup);
        if (OB_SUCCESS == ret)
        {
          bidword_id_array[i] = adgroup.start_id_ + random() 
                                % (adgroup.end_id_ - adgroup.start_id_);
          act_bidword_cnt++;
        }
      }

      if (OB_SUCCESS == ret)
      {
        bidword_cnt = act_bidword_cnt;
      }

      return ret;
    }

    int ObLzQuery::get_random_bidword_id(int64_t& bidword_id)
    {
      int ret = OB_SUCCESS;
      int64_t start_id = 0;
      int64_t row_count = 0;
      ObScanInfo scan_info;
      ObAdgroup adgroup;
      
      ret = get_scan_info(scan_info);
      if (OB_SUCCESS == ret)
      {
        ret = get_cur_scan_adgroup(scan_info, adgroup);
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < MAX_SHOP_TYPE; ++i)
        {
          if ((adgroup.start_id_ + i) % MAX_SHOP_TYPE == scan_info.shop_type_)
          {
            start_id = adgroup.start_id_ + i;
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_count = calc_row_count(adgroup.end_id_, start_id, MAX_SHOP_TYPE);
        bidword_id = start_id + random() % row_count * MAX_SHOP_TYPE;
      }

      return ret;
    }

    int64_t ObLzQuery::get_not_equl_val_colum_idx()
    {
      int64_t ret_val = 0;

      do
      {
        ret_val = random() % EFFECT_COUNT;
      } while (is_orderby_equal_val_column(ret_val));

      return ret_val;
    }

    void ObLzQuery::get_start_rowkey_struct(ObLZRowkey& rowkey, int64_t& day_count)
    {
      if (rowkey.cur_unit_id_ == 0)
      {
        rowkey.cur_unit_id_ = get_random_unit_id();
      }

      if (rowkey.cur_date_ == 0)
      {
        rowkey.cur_date_ = get_random_start_date(day_count);
      }

      if (rowkey.cur_campaign_id_ == 0)
      {
        rowkey.cur_campaign_id_ = rowkey.cur_unit_id_;
      }

      //TODO: support adgroup and bidword, creative
      rowkey.cur_adgroup_id_ = 0;
      rowkey.cur_key_type_ = 0;
      rowkey.cur_key_id_ = 0;
      rowkey.cur_creative_id_ = 0;
    }

    void ObLzQuery::get_end_rowkey_struct(
        const ObLZRowkey& start_rowkey, 
        const int64_t day_count, 
        ObLZRowkey& end_rowkey)
    {
      end_rowkey.cur_unit_id_ = start_rowkey.cur_unit_id_;
      end_rowkey.cur_date_ = start_rowkey.cur_date_ + day_count - 1;
      if (start_rowkey.cur_unit_id_ == start_rowkey.cur_campaign_id_)
      {
        end_rowkey.cur_campaign_id_ = start_rowkey.cur_campaign_id_;
      }
      else if (start_rowkey.cur_campaign_id_ == 0)
      {
        end_rowkey.cur_campaign_id_ = -1;
      }

      //TODO: support adgroup and bidword, creative
      end_rowkey.cur_adgroup_id_ = -1;
      end_rowkey.cur_key_type_ = -1;
      end_rowkey.cur_key_id_ = -1;
      end_rowkey.cur_creative_id_ = -1;
    }

    int ObLzQuery::reset_extra_info()
    {
      int ret = OB_SUCCESS;

      ObScanExtraInfo* extra_info = GET_TSI_MULT(ObScanExtraInfo,TSI_OLAP_SCAN_EXTRA_INFO_1);
      if (NULL == extra_info)
      {
        TBSYS_LOG(WARN, "failed to thread local ObScanExtraInfo");
        ret = OB_ERROR;
      }
      else
      {
        extra_info->reset();  
      }

      return ret;
    }

    void ObLzQuery::init_version_range(ObVersionRange& ver_range)
    {
      ver_range.start_version_ = 0;
      ver_range.border_flag_.set_inclusive_start();
      ver_range.border_flag_.set_max_value();
    }

    int ObLzQuery::build_rowkey(
        const ObLZRowkey& rowkey, 
        ObObj* rowkey_objs, 
        ObRowkey& ret_rowkey)
    {
      int ret         = OB_SUCCESS;
      int64_t cur_id  = rowkey.cur_key_type_ == 0 
        ? rowkey.cur_key_id_ : rowkey.cur_creative_id_;

      rowkey_objs[0].set_int(rowkey.cur_unit_id_);
      rowkey_objs[1].set_int(rowkey.cur_date_);
      rowkey_objs[2].set_int(rowkey.cur_campaign_id_);
      rowkey_objs[3].set_int(rowkey.cur_adgroup_id_);
      rowkey_objs[4].set_int(rowkey.cur_key_type_);
      rowkey_objs[5].set_int(cur_id);
      ret_rowkey.assign(rowkey_objs, MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT);

      return ret;
    }

    int ObLzQuery::add_scan_range(ObScanParam& scan_param)
    {
      int ret                       = OB_SUCCESS;
      int64_t day_count             = param_.get_read_day_count();
      ObThreadRowkeyObjs* rowkey_objs = GET_TSI_MULT(ObThreadRowkeyObjs,TSI_OLAP_THREAD_ROW_KEY_1);
      ObLZRowkey* start_rowkey        = GET_TSI_MULT(ObLZRowkey, START_ROWKEY_NO);
      ObLZRowkey* end_rowkey          = GET_TSI_MULT(ObLZRowkey, END_ROWKEY_NO);
      ObNewRange range;
      ObString table_name;

      if (NULL == start_rowkey || NULL == end_rowkey)
      {
        TBSYS_LOG(ERROR, "failed to get thread rowkey buffer");
        ret = OB_ERROR;
      }
      else
      {
        start_rowkey->cur_unit_id_ = 0;
        start_rowkey->cur_date_ = 0;
        start_rowkey->cur_campaign_id_ = 0;
        get_start_rowkey_struct(*start_rowkey, day_count);
        get_end_rowkey_struct(*start_rowkey, day_count, *end_rowkey);

        ret = build_rowkey(*start_rowkey, rowkey_objs->start_rowkey_objs_, 
                           range.start_key_);
      }

      if (OB_SUCCESS == ret)
      {
        range.border_flag_.set_inclusive_start();
        ret = build_rowkey(*end_rowkey, rowkey_objs->end_rowkey_objs_, 
                           range.end_key_);
        if (OB_SUCCESS == ret)
        {
          range.border_flag_.set_inclusive_end();
        }
      }

      if (OB_SUCCESS == ret)
      {
        table_name = schema_.get_lz_name();
        ret = scan_param.set(OB_INVALID_ID, table_name, range);
      }

      return ret;
    }

    int ObLzQuery::add_basic_scan_column(ObScanParam& scan_param)
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* lz_schema        = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name_str           = NULL;
      int32_t column_size                   = 0 ;
      ObString column_name;

      lz_schema = schema_.get_lz_schema();
      if (NULL != lz_schema)
      {
        column_schema = schema_.get_schema_manager().
          get_table_schema(lz_schema->get_table_id(), column_size);
        for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
        {
          if (NULL != &column_schema[i])
          {
            column_name_str = column_schema[i].get_name();
            column_name.assign(const_cast<char*>(column_name_str), 
                               static_cast<int32_t>(strlen(column_name_str)));
            ret = scan_param.add_column(column_name);
          }
          else
          {
            ret = OB_ERROR;
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "lz table schema is invalid, lz_schema=%p", 
                  lz_schema);
        ret = OB_ERROR;
      }

      return ret;
    }

    void ObLzQuery::set_read_param(ObScanParam& scan_param)
    {
      ObVersionRange ver_range;

      init_version_range(ver_range);
      scan_param.set_version_range(ver_range);
      scan_param.set_scan_size(OB_MAX_PACKET_LENGTH);
      scan_param.set_is_result_cached(true);
    }

    int ObLzQuery::build_basic_scan_param(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      
      ret = add_scan_range(scan_param);
      if (OB_SUCCESS == ret)
      {
        set_read_param(scan_param);
        ret = add_basic_scan_column(scan_param);
      }

      return ret;
    }

    int ObLzQuery::add_basic_aggreate_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      ret = groupby_param.add_aggregate_column(impression, impression, SUM);
      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(finclick, finclick, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(finprice, finprice, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(position_sum, position_sum, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(inner_ipv, inner_ipv, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(item_coll_num, item_coll_num, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(shop_coll_num, shop_coll_num, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(gmv_direct_num, gmv_direct_num, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(gmv_direct_amt, gmv_direct_amt, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(gmv_indirect_num, gmv_indirect_num, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(gmv_indirect_amt, gmv_indirect_amt, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(alipay_direct_num, alipay_direct_num, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(alipay_direct_amt, alipay_direct_amt, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(alipay_indirect_num, alipay_indirect_num, SUM);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(alipay_indirect_amt, alipay_indirect_amt, SUM);
      }

      return ret;
    }

    int ObLzQuery::add_basic_composite_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      ret = groupby_param.add_column(finclick_rate_expr, finclick_rate);
      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(avg_finprice_expr, avg_finprice);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(avg_rank_expr, avg_rank);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(efficiency_expr, efficiency);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(alipay_num_expr, alipay_num);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(alipay_amt_expr, alipay_amt);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(coll_num_expr, coll_num);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(roc_expr, roc);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_column(effect_rank_expr, effect_rank);
      }

      return ret;
    }

    int ObLzQuery::build_groupby_param(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      ret = add_basic_aggreate_column(scan_param);
      if (OB_SUCCESS == ret)
      {
        ret = add_basic_composite_column(scan_param);
      }

      return ret;
    }

    int ObLzQuery::check_basic_effect_column(
      const ObCellInfo& cell_info, const int index, const int64_t sum_val)
    {
      int ret       = OB_SUCCESS;
      int64_t intv  = 0;

      if (ObIntType != cell_info.value_.get_type())
      {
        TBSYS_LOG(WARN, "invalid cell type, must be ObIntType, type=%d", 
                  cell_info.value_.get_type());
        ret = OB_ERROR;
      }
      else 
      {
        ret = cell_info.value_.get_int(intv);
      }

      if (OB_SUCCESS == ret)
      {
        switch (index)
        {
        case IMPRESSION:
        case FINCLICK:
        case FINPRICE:
        case POSITION_SUM:
        case INNER_IPV:
        case ITEM_COLL_NUM:
        case SHOP_COLL_NUM:
        case GMV_DIRECT_NUM:
        case GMV_DIRECT_AMT:
        case GMV_INDIRECT_NUM:
        case GMV_INDIRECT_AMT:
        case ALIPAY_DIRECT_NUM:
        case ALIPAY_DIRECT_AMT:
        case ALIPAY_INDIRECT_NUM:
        case ALIPAY_INDIRECT_AMT:
          if (intv != sum_val)
          {
            TBSYS_LOG(WARN, "wrong result, column_index=%d, column_name=%.*s, "
                            "ret_val=%ld, exp_val=%ld",
                      index, cell_info.column_name_.length(), 
                      cell_info.column_name_.ptr(), intv, sum_val);
            ret = OB_ERROR;
          }
          break;
    
        //composite columns
        case FINCLICK_RATE:
        case AVG_FINPRICE:
        case AVG_RANK:
          if (intv != 1)
          {
            TBSYS_LOG(WARN, "wrong result, column_index=%d, column_name=%.*s, "
                            "ret_val=%ld, exp_val=%d",
                      index, cell_info.column_name_.length(), 
                      cell_info.column_name_.ptr(), intv, 1);
            ret = OB_ERROR;
          }
          break;
  
        case EFFICIENCY:
          if (intv != sum_val)
          {
            TBSYS_LOG(WARN, "wrong result, column_index=%d, column_name=%.*s, "
                            "ret_val=%ld, exp_val=%ld",
                      index, cell_info.column_name_.length(), 
                      cell_info.column_name_.ptr(), intv, sum_val);
            ret = OB_ERROR;
          }
          break;
  
        case ALIPAY_NUM:
        case ALIPAY_AMT:
        case COLL_NUM:
          if (intv != 2 * sum_val)
          {
            TBSYS_LOG(WARN, "wrong result, column_index=%d, column_name=%.*s, "
                            "ret_val=%ld, exp_val=%ld",
                      index, cell_info.column_name_.length(), 
                      cell_info.column_name_.ptr(), intv, 2 * sum_val);
            ret = OB_ERROR;
          }
          break;
        
        case ROC:
          if (intv != 2)
          {
            TBSYS_LOG(WARN, "wrong result, column_index=%d, column_name=%.*s, "
                            "ret_val=%ld, exp_val=%d",
                      index, cell_info.column_name_.length(), 
                      cell_info.column_name_.ptr(), intv, 2);
            ret = OB_ERROR;
          }
          break;

        case EFFECT_RANK:
            if (intv != 2 * sum_val)
          {
            TBSYS_LOG(WARN, "wrong result, column_index=%d, column_name=%.*s, "
                            "ret_val=%ld, exp_val=%ld",
                      index, cell_info.column_name_.length(), 
                      cell_info.column_name_.ptr(), intv, 2 * sum_val);
            ret = OB_ERROR;
          }
          break;

        default:
          TBSYS_LOG(WARN, "unknown effect column type, type=%d", index);
          ret = OB_ERROR;
          break;
        }
      }

      return ret;
    }

    int ObLzQuery::check_cell(
      const ObCellInfo& cell_info,
      const int64_t column_index,
      const int64_t exp_val)
    {
      int ret       = OB_SUCCESS;
      int64_t intv  = 0;

      if (ObIntType != cell_info.value_.get_type())
      {
        TBSYS_LOG(WARN, "invalid cell type, must be ObIntType, type=%d", 
                  cell_info.value_.get_type());
        ret = OB_ERROR;
      }
      else 
      {
        ret = cell_info.value_.get_int(intv);
        if (OB_SUCCESS == ret && intv != exp_val)
        {
          TBSYS_LOG(WARN, "wrong result, column_index=%ld, column_name=%.*s, "
                          "ret_val=%ld, exp_val=%ld", 
                    column_index, cell_info.column_name_.length(), 
                    cell_info.column_name_.ptr(), intv, exp_val);
          ret = OB_ERROR;
        }
      }
     
      return ret;
    }

    int ObLzQuery::check_one_cell(
      const ObCellInfo& cell_info,
      const int64_t groupby_col_cnt,
      const int64_t column_index,
      const int64_t exp_val,
      const int64_t sum_val)
    {
      int ret = OB_SUCCESS;

      if (column_index < groupby_col_cnt)
      {
        ret = check_cell(cell_info, column_index, exp_val);
      }
      else
      {
        check_basic_effect_column(cell_info, static_cast<int32_t>(column_index - groupby_col_cnt), sum_val);
      }

      return ret;
    }

    int ObLzQuery::get_cur_scan_campaign(ObCampaign& campaign)
    {
      int ret                 = OB_SUCCESS;
      ObLZRowkey* start_rowkey  = GET_TSI_MULT(ObLZRowkey, START_ROWKEY_NO);

      if (NULL == start_rowkey)
      {
        TBSYS_LOG(ERROR, "failed to get thread start rowkey");
        ret = OB_ERROR;
      }
      else
      {
        ret = meta_.get_campaign(start_rowkey->cur_unit_id_, 
                                 start_rowkey->cur_campaign_id_, 
                                 campaign);
      }

      return ret;
    }

    int ObLzQuery::get_scan_info(ObScanInfo& scan_info)
    {
      int ret                 = OB_SUCCESS;
      ObLZRowkey* start_rowkey  = GET_TSI_MULT(ObLZRowkey, START_ROWKEY_NO);
      ObLZRowkey* end_rowkey    = GET_TSI_MULT(ObLZRowkey, END_ROWKEY_NO);
      ObScanExtraInfo* extra_info = GET_TSI_MULT(ObScanExtraInfo,TSI_OLAP_SCAN_EXTRA_INFO_1);

      if (NULL == start_rowkey || NULL == end_rowkey || NULL == extra_info)
      {
        TBSYS_LOG(ERROR, "failed to get thread rowkey");
        ret = OB_ERROR;
      }
      else
      {
        scan_info.platform_id_ = extra_info->platform_id_;
        scan_info.pvtype_id_ = extra_info->pvtype_id_;
        scan_info.shop_type_ = extra_info->shop_type_;
        scan_info.orderby_col_idx_ = extra_info->orderby_col_idx_;
        scan_info.adgroup_id_ = extra_info->adgroup_id_;
        scan_info.start_date_ = start_rowkey->cur_date_;
        scan_info.day_count_ = end_rowkey->cur_date_ - start_rowkey->cur_date_ + 1;
        scan_info.bidword_id_ = extra_info->bidword_id_;
        scan_info.batch_scan_info_ = extra_info->batch_scan_info_;

        ret = get_cur_scan_campaign(scan_info.campaign_);
      }

      return ret;
    }

    int ObLzQuery::check_row_and_cell_num(
      const ObScanner& scanner, const int64_t exp_row_num, 
      const int64_t exp_cell_num)
    {
      int ret = OB_SUCCESS;

      if (exp_row_num != scanner.get_row_num() 
          || exp_cell_num != scanner.get_cell_num())
      {
        TBSYS_LOG(WARN, "returns wrong row count or column count, row_count=%ld, "
                        "exp_row_count=%ld, column_count=%ld, exp_column_count=%ld",
                  scanner.get_row_num(), exp_row_num, 
                  scanner.get_cell_num(), exp_cell_num);
        ret = OB_ERROR;
      }

      return ret;
    }

    void ObLzQuery::print_brief_result(
      const char* test_name, const ObScanner& scanner,
      const ObScanInfo& scan_info, const int64_t time,
      const int64_t org_row_cnt)
    {
      stat_.add_scan_row((uint64_t)org_row_cnt);
      if (log_level_ > 0 || time > WARN_RESPONSE_TIME)
      {
        fprintf(stderr, "\n%s: time=%ld, data_version=%ld, ret_row_cnt=%ld, "
                        "ret_cell_cnt=%ld, org_row_cnt=%ld\n", 
                test_name, time, scanner.get_data_version(), 
                scanner.get_row_num(), scanner.get_cell_num(), org_row_cnt);
        scan_info.display();
      }
    }

    void ObLzQuery::print_detail_result(const ObScanner& scanner)
    {
      int err = OB_SUCCESS;
      ObScanInfo scan_info;

      if (log_level_ > 1)
      {
        display_scanner(scanner);
      }

      err = get_scan_info(scan_info);
      if (OB_SUCCESS == err)
      {
        scan_info.campaign_.display();
        scan_info.display();
      }
      fprintf(stderr, "test: %s\n", get_test_name());
    }

    int ObLzQuery::add_thedate_groupby_orderby(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      ret = scan_param.add_orderby_column(thedate, ObScanParam::DESC);
      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(thedate);
      }

      return ret;
    }

    int ObLzQuery::build_batch_scan_filter(
      char* buf, const int64_t buf_len, int64_t& pos, 
      const ObBatchScanInfo& batch_scan_info)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", 
                  buf, buf_len, pos);
        ret = OB_ERROR;
      }
      else
      {
        if (batch_scan_info.adgroup_cnt_ > 0 
            && 0 == batch_scan_info.bidword_cnt_ 
            && 0 == batch_scan_info.creative_cnt_)
        {
          for (int64_t i = 0; i < batch_scan_info.adgroup_cnt_; ++i)
          {
            if (0 == i && 0 == pos)
            {
              pos += sprintf(buf + pos, "(`adgroup_id` = %ld)", 
                             batch_scan_info.adgroup_id_array_[i]);
            }
            else
            {
              pos += sprintf(buf + pos, " OR (`adgroup_id` = %ld)", 
                             batch_scan_info.adgroup_id_array_[i]);
            }
          }
        }
        else if (batch_scan_info.adgroup_cnt_ > 0 
                 && batch_scan_info.bidword_cnt_ > 0)
        {
          for (int64_t i = 0; i < batch_scan_info.bidword_cnt_; ++i)
          {
            if (0 == i && 0 == pos)
            {
              pos += sprintf(buf + pos, "((`adgroup_id` = %ld) "
                                        "AND (`bidword_id` = %ld))", 
                             batch_scan_info.adgroup_id_array_[i],
                             batch_scan_info.bidword_id_array_[i]);
            }
            else
            {
              pos += sprintf(buf + pos, " OR ((`adgroup_id` = %ld) "
                                        "AND (`bidword_id` = %ld))", 
                             batch_scan_info.adgroup_id_array_[i],
                             batch_scan_info.bidword_id_array_[i]);
            }
          }
        }
        else if (batch_scan_info.adgroup_cnt_ > 0 
                 && batch_scan_info.creative_cnt_ > 0)
        {
          for (int64_t i = 0; i < batch_scan_info.creative_cnt_; ++i)
          {
            if (0 == i && 0 == pos)
            {
              pos += sprintf(buf + pos, "((`adgroup_id` = %ld) "
                                        "AND (`creative_id` = %ld))", 
                             batch_scan_info.adgroup_id_array_[i],
                             batch_scan_info.creative_id_array_[i]);
            }
            else
            {
              pos += sprintf(buf + pos, " OR ((`adgroup_id` = %ld) "
                                        "AND (`creative_id` = %ld))", 
                             batch_scan_info.adgroup_id_array_[i],
                             batch_scan_info.creative_id_array_[i]);
            }
          }
        }
      }

      return ret;
    }

    int64_t ObLzQuery::calc_row_count(
      const int64_t end_val, const int64_t start_val, 
      const int64_t interval)
    {
      int64_t row_count = (end_val - start_val) / interval
        + (((end_val - start_val) % interval > 0) ? 1 : 0);

      return row_count;
    }

    int64_t ObLzQuery::calc_sum_val(
      const int64_t end_val, const int64_t start_val, 
      const int64_t interval, const int64_t day_count)
    {
      int64_t ret_val   = 0;
      int64_t row_count = calc_row_count(end_val, start_val, interval);

      ret_val = (start_val + start_val + (row_count - 1) * interval) 
                * row_count * day_count;

      return ret_val;
    }

    int64_t ObLzQuery::calc_adgroup_shoptype_sum_val(
      const ObAdgroup& adgroup, const int64_t day_count, 
      const int64_t shop_type)
    {
      int64_t ret_val   = 0;
      int64_t start_val = 0;
      int64_t interval  = MAX_SHOP_TYPE;

      if (shop_type == MAX_SHOP_TYPE)
      {
        interval = 1;
        start_val = adgroup.start_id_;
      }
      else
      {
        for (int64_t i = 0; i < interval; ++i)
        {
          if ((adgroup.start_id_ + i) % MAX_SHOP_TYPE == shop_type)
          {
            start_val = adgroup.start_id_ + i;
            break;
          }
        }
      }

      ret_val = calc_sum_val(adgroup.end_id_, start_val, interval, day_count);

      return ret_val;
    }

    bool ObLzQuery::is_orderby_equal_val_column(const int64_t orderby_col_idx)
    {
      return (FINCLICK_RATE == orderby_col_idx
              || AVG_FINPRICE == orderby_col_idx 
              || AVG_RANK == orderby_col_idx  
              || ROC == orderby_col_idx);
    }

    int ObLzQuery::get_cur_scan_adgroup(
      const ObScanInfo& scan_info, ObAdgroup& adgroup)
    {
      int ret = OB_SUCCESS;

      ret = meta_.get_adgroup(scan_info.campaign_.campaign_id_, 
                              scan_info.campaign_.campaign_id_, 
                              scan_info.adgroup_id_, adgroup);

      return ret;
    }

    int ObLzQuery::add_special_column(ObScanParam& scan_param)
    {
      int ret = OB_ERROR;

      UNUSED(scan_param);
      TBSYS_LOG(WARN, "add_special_column check not implementation");

      return ret; 
    }

    int ObLzQuery::prepare(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      ret = build_basic_scan_param(scan_param);
      if (OB_SUCCESS == ret)
      {
        ret = add_special_column(scan_param);
      }
      
      if (OB_SUCCESS == ret)
      {
        ret = build_groupby_param(scan_param);
      }

      return ret;
    }

    int ObLzQuery::scan(ObScanParam& scan_param, ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      int64_t start_time = 0;
      int64_t end_time = 0;

      if (NULL == rowkey_meta_ && OB_SUCCESS != (ret = init()))
      {
        TBSYS_LOG(WARN, "failed to init rowkey meta:");
      }
      else if (rowkey_meta_->max_unit_id_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid rowkey meta:");
        rowkey_meta_->display();
        ret = OB_ERROR;
      }
      else
      {
        scan_param.reset();
        scanner.reset();
  
        ret = prepare(scan_param);
        if (OB_SUCCESS == ret)
        {
          start_time = get_cur_microseconds_time();
          ret = client_.ms_scan(scan_param, scanner);
          end_time = get_cur_microseconds_time();
          stat_.add_scan_opt(1);
        }
  
        //check the scan result
        if (OB_SUCCESS == ret)
        {
          ret = check(scanner, end_time - start_time);
        }
        else if (OB_DATA_NOT_SERVE == ret || OB_RESPONSE_TIME_OUT == ret)
        {
          //read a key in hole of key range or timeout,skip it
          ret = OB_SUCCESS;
        }

        if (OB_SUCCESS != ret)
        {
          print_detail_result(scanner);
        }
      }

      return ret;
    }

    int ObLzQuery::check(const ObScanner& scanner, const int64_t time)
    {
      int ret = OB_ERROR;

      UNUSED(scanner);
      UNUSED(time);
      TBSYS_LOG(WARN, "check not implementation");

      return ret;    
    }

    const char* ObLzQuery::get_test_name()
    {
      return "INVALID_NAME";
    }
  } // end namespace olapdrive
} // end namespace oceanbase
