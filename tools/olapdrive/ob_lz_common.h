/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_lz_common.h for define common part of lz application. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_LZ_COMMON_H
#define OCEANBASE_LZ_COMMON_H

#include "common/ob_string.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    static const int64_t MAX_PLATFORM_ID = 2;
    static const int64_t MAX_PV_TYPE = 3;
    static const int64_t MAX_SHOP_TYPE = 3;

    static const int64_t MAX_COMPAIGN_ADGROUP_COUNT = 10000;
    static const int64_t MAX_COMPAIGN_ROW_COUNT = 500000;
    static const int64_t MAX_ADGROUP_KEY_COUNT = 400;
    static const int64_t MAX_ADGROUP_CREATIVE_COUNT = 400;

    static const int64_t MAX_DIST_COUNT = 11;

    //origin columns
    static const common::ObString shop_id(static_cast<int32_t>(strlen("shop_id")), static_cast<int32_t>(strlen("shop_id")), 
                                          (char*)"shop_id");
    static const common::ObString customer_id(static_cast<int32_t>(strlen("customer_id")), 
                                              static_cast<int32_t>(strlen("customer_id")), (char*)"customer_id");
    static const common::ObString thedate(static_cast<int32_t>(strlen("thedate")), 
                                          static_cast<int32_t>(strlen("thedate")), (char*)"thedate");
    static const common::ObString campaign_id(static_cast<int32_t>(strlen("campaign_id")), 
                                              static_cast<int32_t>(strlen("campaign_id")), (char*)"campaign_id");
    static const common::ObString adgroup_id(static_cast<int32_t>(strlen("adgroup_id")), 
                                             static_cast<int32_t>(strlen("adgroup_id")), (char*)"adgroup_id");
    static const common::ObString bidword_id(static_cast<int32_t>(strlen("bidword_id")), 
                                             static_cast<int32_t>(strlen("bidword_id")), (char*)"bidword_id");
    static const common::ObString creative_id(static_cast<int32_t>(strlen("creative_id")), 
                                              static_cast<int32_t>(strlen("creative_id")), (char*)"creative_id");
    static const common::ObString shop_type(static_cast<int32_t>(strlen("shop_type")), 
                                            static_cast<int32_t>(strlen("shop_type")), (char*)"shop_type");
    static const common::ObString platform_id(static_cast<int32_t>(strlen("platform_id")), 
                                              static_cast<int32_t>(strlen("platform_id")), (char*)"platform_id");
    static const common::ObString pvtype_id(static_cast<int32_t>(strlen("pvtype_id")), 
                                            static_cast<int32_t>(strlen("pvtype_id")), (char*)"pvtype_id");

    static const common::ObString impression(static_cast<int32_t>(strlen("impression")), 
                                             static_cast<int32_t>(strlen("impression")), (char*)"impression");
    static const common::ObString finclick(static_cast<int32_t>(strlen("finclick")), 
                                           static_cast<int32_t>(strlen("finclick")), (char*)"finclick");
    static const common::ObString finprice(static_cast<int32_t>(strlen("finprice")), 
                                           static_cast<int32_t>(strlen("finprice")), (char*)"finprice");
    static const common::ObString position_sum(static_cast<int32_t>(strlen("position_sum")), 
                                               static_cast<int32_t>(strlen("position_sum")), (char*)"position_sum");
    static const common::ObString inner_ipv(static_cast<int32_t>(strlen("inner_ipv")), 
                                            static_cast<int32_t>(strlen("inner_ipv")), (char*)"inner_ipv");
    static const common::ObString item_coll_num(static_cast<int32_t>(strlen("item_coll_num")), 
                                                static_cast<int32_t>(strlen("item_coll_num")), (char*)"item_coll_num");
    static const common::ObString shop_coll_num(static_cast<int32_t>(strlen("shop_coll_num")), 
                                                static_cast<int32_t>(strlen("shop_coll_num")), (char*)"shop_coll_num");
    static const common::ObString gmv_direct_num(static_cast<int32_t>(strlen("gmv_direct_num")), 
                                                 static_cast<int32_t>(strlen("gmv_direct_num")), (char*)"gmv_direct_num");
    static const common::ObString gmv_direct_amt(static_cast<int32_t>(strlen("gmv_direct_amt")), 
                                                 static_cast<int32_t>(strlen("gmv_direct_amt")), (char*)"gmv_direct_amt");
    static const common::ObString gmv_indirect_num(static_cast<int32_t>(strlen("gmv_indirect_num")), 
                                                   static_cast<int32_t>(strlen("gmv_indirect_num")), (char*)"gmv_indirect_num");
    static const common::ObString gmv_indirect_amt(static_cast<int32_t>(strlen("gmv_indirect_amt")), 
                                                   static_cast<int32_t>(strlen("gmv_indirect_amt")), (char*)"gmv_indirect_amt");
    static const common::ObString alipay_direct_num(static_cast<int32_t>(strlen("alipay_direct_num")), 
                                                    static_cast<int32_t>(strlen("alipay_direct_num")), (char*)"alipay_direct_num");
    static const common::ObString alipay_direct_amt(static_cast<int32_t>(strlen("alipay_direct_amt")), 
                                                    static_cast<int32_t>(strlen("alipay_direct_amt")), (char*)"alipay_direct_amt");
    static const common::ObString alipay_indirect_num(static_cast<int32_t>(strlen("alipay_indirect_num")), 
                                                      static_cast<int32_t>(strlen("alipay_indirect_num")), (char*)"alipay_indirect_num");
    static const common::ObString alipay_indirect_amt(static_cast<int32_t>(strlen("alipay_indirect_amt")), 
                                                      static_cast<int32_t>(strlen("alipay_indirect_amt")), (char*)"alipay_indirect_amt");

    //composite columns
    static char* finclick_rate_str = (char*)"`finclick` / `impression`";
    static const common::ObString finclick_rate_expr(static_cast<int32_t>(strlen(finclick_rate_str)), 
                                                     static_cast<int32_t>(strlen(finclick_rate_str)), finclick_rate_str);
    static const common::ObString finclick_rate(static_cast<int32_t>(strlen("finclick_rate")), 
                                                static_cast<int32_t>(strlen("finclick_rate")), (char*)"finclick_rate");

    static char* avg_finprice_str = (char*)"`finprice` / `finclick`";
    static const common::ObString avg_finprice_expr(static_cast<int32_t>(strlen(avg_finprice_str)), 
                                                    static_cast<int32_t>(strlen(avg_finprice_str)), avg_finprice_str);
    static const common::ObString avg_finprice(static_cast<int32_t>(strlen("avg_finprice")), 
                                               static_cast<int32_t>(strlen("avg_finprice")), (char*)"avg_finprice");

    static char* avg_rank_str = (char*)"`position_sum` / `impression`";
    static const common::ObString avg_rank_expr(static_cast<int32_t>(strlen(avg_rank_str)), 
                                                static_cast<int32_t>(strlen(avg_rank_str)), avg_rank_str);
    static const common::ObString avg_rank(static_cast<int32_t>(strlen("avg_rank")), 
                                           static_cast<int32_t>(strlen("avg_rank")), (char*)"avg_rank");

    static char* efficiency_str = (char*)"`finclick` * `finclick` / `finprice`";
    static const common::ObString efficiency_expr(static_cast<int32_t>(strlen(efficiency_str)), 
                                                  static_cast<int32_t>(strlen(efficiency_str)), efficiency_str);
    static const common::ObString efficiency(static_cast<int32_t>(strlen("efficiency")), 
                                            static_cast<int32_t>(strlen("efficiency")), (char*)"efficiency");

    static char* alipay_num_str = (char*)"`alipay_direct_num` + `alipay_indirect_num`";
    static const common::ObString alipay_num_expr(static_cast<int32_t>(strlen(alipay_num_str)), 
                                                  static_cast<int32_t>(strlen(alipay_num_str)), alipay_num_str);
    static const common::ObString alipay_num(static_cast<int32_t>(strlen("alipay_num")), 
                                             static_cast<int32_t>(strlen("alipay_num")), (char*)"alipay_num");

    static char* alipay_amt_str = (char*)"`alipay_direct_amt` + `alipay_indirect_amt`";
    static const common::ObString alipay_amt_expr(static_cast<int32_t>(strlen(alipay_amt_str)), 
                                                  static_cast<int32_t>(strlen(alipay_amt_str)), alipay_amt_str);
    static const common::ObString alipay_amt(static_cast<int32_t>(strlen("alipay_amt")), 
                                             static_cast<int32_t>(strlen("alipay_amt")), (char*)"alipay_amt");

    static char* coll_num_str = (char*)"`item_coll_num` + `shop_coll_num`";
    static const common::ObString coll_num_expr(static_cast<int32_t>(strlen(coll_num_str)), 
                                                static_cast<int32_t>(strlen(coll_num_str)), coll_num_str);
    static const common::ObString coll_num(static_cast<int32_t>(strlen("coll_num")), 
                                           static_cast<int32_t>(strlen("coll_num")), (char*)"coll_num");

    static char* roc_str = (char*)"(`alipay_direct_num` + `alipay_indirect_num`) / `finclick`";
    static const common::ObString roc_expr(static_cast<int32_t>(strlen(roc_str)), 
                                           static_cast<int32_t>(strlen(roc_str)), roc_str);
    static const common::ObString roc(static_cast<int32_t>(strlen("roc")), static_cast<int32_t>(strlen("roc")), (char*)"roc");

    static char* effect_rank_str = (char*)"(`alipay_direct_num` + `alipay_indirect_num`) * `finclick` / `finprice`";
    static const common::ObString effect_rank_expr(static_cast<int32_t>(strlen(effect_rank_str)), 
                                                   static_cast<int32_t>(strlen(effect_rank_str)), effect_rank_str);
    static const common::ObString effect_rank(static_cast<int32_t>(strlen("effect_rank")), 
                                              static_cast<int32_t>(strlen("effect_rank")), (char*)"effect_rank");

    //temp
    static const common::ObString impression_sum(static_cast<int32_t>(strlen("impression_sum")), 
                                                 static_cast<int32_t>(strlen("impression_sum")), (char*)"impression_sum");
    static const common::ObString count(static_cast<int32_t>(strlen("count")), static_cast<int32_t>(strlen("count")), (char*)"count");

    //filter
    static char* impression_filter_str = (char*)"`impression` > 0";
    static const common::ObString impression_filter_expr(static_cast<int32_t>(strlen(impression_filter_str)), 
                                                 static_cast<int32_t>(strlen(impression_filter_str)), 
                                                 impression_filter_str);

    enum ObEffectColumn
    {
      IMPRESSION = 0,
      FINCLICK,
      FINPRICE,
      POSITION_SUM,
      INNER_IPV,
      ITEM_COLL_NUM,
      SHOP_COLL_NUM,
      GMV_DIRECT_NUM,
      GMV_DIRECT_AMT,
      GMV_INDIRECT_NUM,
      GMV_INDIRECT_AMT,
      ALIPAY_DIRECT_NUM,
      ALIPAY_DIRECT_AMT,
      ALIPAY_INDIRECT_NUM,
      ALIPAY_INDIRECT_AMT,

      //composite columns
      FINCLICK_RATE,
      AVG_FINPRICE,
      AVG_RANK,
      EFFICIENCY,
      ALIPAY_NUM,
      ALIPAY_AMT,
      COLL_NUM,
      ROC,
      EFFECT_RANK,
      EFFECT_COUNT
    };

    const common::ObString EFFECT_COLUMN_NAME[] =
    {
      impression,
      finclick,
      finprice,
      position_sum,
      inner_ipv,
      item_coll_num,
      shop_coll_num,
      gmv_direct_num,
      gmv_direct_amt,
      gmv_indirect_num,
      gmv_indirect_amt,
      alipay_direct_num,
      alipay_direct_amt,
      alipay_indirect_num,
      alipay_indirect_amt,

      //composite columns
      finclick_rate,
      avg_finprice,
      avg_rank,
      efficiency,
      alipay_num,
      alipay_amt,
      coll_num,
      roc,
      effect_rank
    };

    #define FOREACH_CELL(scanner)       \
        ObCellInfo* cell_info = NULL;   \
        bool is_row_changed   = false;  \
        int64_t column_index  = 0;      \
        int64_t row_index     = 0;      \
        ObScannerIterator iter;         \
        for (iter = scanner.begin();    \
             iter != scanner.end() && OB_SUCCESS == ret; \
             iter++, column_index++)    

    #define GET_CELL()                                \
        iter.get_cell(&cell_info, &is_row_changed);   \
        if (NULL == cell_info)                        \
        {                                             \
          TBSYS_LOG(WARN, "get null cell_info");      \
          ret = OB_ERROR;                             \
          break;                                      \
        }                                             \
        else if (is_row_changed)                      \
        {                                             \
          column_index = 0;                           \
          row_index++;                                \
        }

    #define START_BUILD_EXTRA_INFO(scan_param)                      \
        int64_t filter_len  = 0;                                    \
        ObScanExtraInfo* extra_info = GET_TSI_MULT(ObScanExtraInfo,TSI_OLAP_SCAN_EXTRA_INFO_1);     \
        common::ObString filter_string;                             \
        if (NULL == extra_info)                                     \
        {                                                           \
          TBSYS_LOG(WARN, "failed to thread local ObScanExtraInfo");\
          ret = OB_ERROR;                                           \
        }                                                           \
        else                                                        \
        {                                                           \
          extra_info->reset();     
          
    #define END_BUILD_EXTRA_INFO(scan_param)                          \
          if (OB_SUCCESS == ret && filter_len > 0)                    \
          {                                                           \
            filter_string.assign(extra_info->filter_str, static_cast<int32_t>(filter_len)); \
            ret = scan_param.add_where_cond(filter_string);           \
          }                                                           \
        }

    struct ObLZRowkey
    {
      ObLZRowkey()
      {
        cur_unit_id_ = -1;
        cur_date_ = -1;
        cur_campaign_id_ = -1;
        cur_adgroup_id_ = -1;
        cur_key_type_ = -1;
        cur_key_id_ = -1;
        cur_creative_id_ = -1;
      }

      void reset()
      {
        cur_unit_id_ = -1;
        cur_date_ = -1;
        cur_campaign_id_ = -1;
        cur_adgroup_id_ = -1;
        cur_key_type_ = 1;
        cur_key_id_ = 0;
        cur_creative_id_ = 0;
      }

      void display() const
      {
        fprintf(stderr, "\tcur_unit_id_: %ld \n"
                        "\tcur_date_: %ld \n"
                        "\tcur_campaign_id_: %ld \n"
                        "\tcur_adgroup_id_: %ld \n"
                        "\tcur_key_type_: %ld \n"
                        "\tcur_key_id_: %ld \n"
                        "\tcur_creative_id_: %ld \n\n",
                cur_unit_id_, cur_date_, cur_campaign_id_,
                cur_adgroup_id_, cur_key_type_, cur_key_id_, cur_creative_id_);
      }

      int64_t cur_unit_id_;
      int64_t cur_date_;
      int64_t cur_campaign_id_;
      int64_t cur_adgroup_id_;
      int64_t cur_key_type_;
      int64_t cur_key_id_;
      int64_t cur_creative_id_;       
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_LZ_COMMON_H
