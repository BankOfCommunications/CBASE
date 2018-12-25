/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_write_worker.cpp for define write worker thread. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "ob_write_worker.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace serialization;
    using namespace client;

    ObWriteWorker::ObWriteWorker(ObClient& client, ObOlapdriveMeta& meta,
                                 ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                                 ObOlapdriveStat& stat)
    : client_(client), 
      meta_(meta), 
      schema_(schema), 
      param_(param),
      stat_(stat), 
      rowkey_meta_(NULL)
    {
      mutator_.reset();
      mutator_row_cnt_ = 0;
      date_status_.reset();
      cur_rowkey_.reset();
      campaign_status_.reset();
      adgroup_status_.reset();
    }

    ObWriteWorker::~ObWriteWorker()
    {

    }

    int ObWriteWorker::init()
    {
      int ret = OB_SUCCESS;
      
      date_status_.write_day_cnt_ = param_.get_write_day_count();
      rowkey_meta_ = &meta_.get_rowkey_meta();
      if (rowkey_meta_->daily_unit_count_ == 0)
      {
        rowkey_meta_->daily_unit_count_ = param_.get_daily_unit_count();
      }
      init_default_row_dist(row_dist_);
      init_default_adgroup_dist(adgroup_dist_);

      return ret;
    }

    void ObWriteWorker::init_default_row_dist(ObCampaignRowDist& row_dist)
    {
      row_dist.dist_[0].start_cnt_ = 50;
      row_dist.dist_[0].end_cnt_ = 50;
      row_dist.dist_[0].prop_ = 1000;

      row_dist.dist_[1].start_cnt_ = 100;
      row_dist.dist_[1].end_cnt_ = 100;
      row_dist.dist_[1].prop_ = 2000;

      row_dist.dist_[2].start_cnt_ = 200;
      row_dist.dist_[2].end_cnt_ = 200;
      row_dist.dist_[2].prop_ = 4000;

      row_dist.dist_[3].start_cnt_ = 300;
      row_dist.dist_[3].end_cnt_ = 300;
      row_dist.dist_[3].prop_ = 5000;

      row_dist.dist_[4].start_cnt_ = 400;
      row_dist.dist_[4].end_cnt_ = 400;
      row_dist.dist_[4].prop_ = 6500;

      row_dist.dist_[5].start_cnt_ = 500;
      row_dist.dist_[5].end_cnt_ = 500;
      row_dist.dist_[5].prop_ = 8100;

      row_dist.dist_[6].start_cnt_ = 600;
      row_dist.dist_[6].end_cnt_ = 1000;
      row_dist.dist_[6].prop_ = 9015;

      row_dist.dist_[7].start_cnt_ = 1000;
      row_dist.dist_[7].end_cnt_ = 2000;
      row_dist.dist_[7].prop_ = 9579;

      row_dist.dist_[8].start_cnt_ = 2000;
      row_dist.dist_[8].end_cnt_ = 4000;
      row_dist.dist_[8].prop_ = 9846;

      row_dist.dist_[9].start_cnt_ = 4000;
      row_dist.dist_[9].end_cnt_ = 10000;
      row_dist.dist_[9].prop_ = 9990;

      row_dist.dist_[10].start_cnt_ = 400000;
      row_dist.dist_[10].end_cnt_ = 400000;
      row_dist.dist_[10].prop_ = 10000;
      row_dist.dist_cnt_ = 11;
    }

    void ObWriteWorker::init_default_adgroup_dist(ObCampaignAdgroupDist& adgroup_dist)
    {
      adgroup_dist.dist_[0].start_cnt_ = 1;
      adgroup_dist.dist_[0].end_cnt_ = 1;
      adgroup_dist.dist_[0].prop_ = 1000;

      adgroup_dist.dist_[1].start_cnt_ = 1;
      adgroup_dist.dist_[1].end_cnt_ = 1;
      adgroup_dist.dist_[1].prop_ = 1000;

      adgroup_dist.dist_[2].start_cnt_ = 1;
      adgroup_dist.dist_[2].end_cnt_ = 1;
      adgroup_dist.dist_[2].prop_ = 4000;

      adgroup_dist.dist_[3].start_cnt_ = 2;
      adgroup_dist.dist_[3].end_cnt_ = 2;
      adgroup_dist.dist_[3].prop_ = 5000;

      adgroup_dist.dist_[4].start_cnt_ = 2;
      adgroup_dist.dist_[4].end_cnt_ = 2;
      adgroup_dist.dist_[4].prop_ = 6500;

      adgroup_dist.dist_[5].start_cnt_ = 10;
      adgroup_dist.dist_[5].end_cnt_ = 10;
      adgroup_dist.dist_[5].prop_ = 8100;

      adgroup_dist.dist_[6].start_cnt_ = 10;
      adgroup_dist.dist_[6].end_cnt_ = 10;
      adgroup_dist.dist_[6].prop_ = 9015;

      adgroup_dist.dist_[7].start_cnt_ = 15;
      adgroup_dist.dist_[7].end_cnt_ = 15;
      adgroup_dist.dist_[7].prop_ = 9579;

      adgroup_dist.dist_[8].start_cnt_ = 5;
      adgroup_dist.dist_[8].end_cnt_ = 5;
      adgroup_dist_.dist_[8].prop_ = 9846;

      adgroup_dist.dist_[9].start_cnt_ = 700;
      adgroup_dist.dist_[9].end_cnt_ = 700;
      adgroup_dist.dist_[9].prop_ = 9990;

      adgroup_dist.dist_[10].start_cnt_ = 1000;
      adgroup_dist.dist_[10].end_cnt_ = 1000;
      adgroup_dist.dist_[10].prop_ = 10000;
      adgroup_dist.dist_cnt_ = 11;
    }

    int ObWriteWorker::set_obj(ObObj& obj, const ObObjType type, 
                               const int64_t value)
    {
      int ret = OB_SUCCESS;

      switch (type)
      {
      case ObIntType:
        obj.set_int(value);
        break;

      case ObDoubleType:
      case ObFloatType:
      case ObDateTimeType:
      case ObPreciseDateTimeType:
      case ObVarcharType:
      case ObNullType:
      case ObSeqType:
      case ObCreateTimeType:
      case ObModifyTimeType:
      case ObExtendType:
      case ObMaxType:
      default:
        TBSYS_LOG(WARN, "wrong object type %d for store rowkey meta", type);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObWriteWorker::write_next_row()
    {
      int ret = OB_SUCCESS;

      if (is_date_changed())
      {
        ret = do_date_change();
      }
      else if (is_campaign_changed())
      {
        ret =do_campaign_change();
      }
      else if (is_adgroup_changed())
      {
        ret = do_adgroup_change();
      }
      else if (is_key_type_changed())
      {
        ret = do_key_type_change();
      }

      if (OB_SUCCESS == ret)
      {
        ret = do_insert();
      }

      return ret;
    }

    int ObWriteWorker::do_insert()
    {
      int ret = OB_SUCCESS;

      ret = build_lz_rowkey();
      if (OB_SUCCESS == ret)
      {
        ret = build_mutator();
      }

      if (OB_SUCCESS == ret 
          && ++mutator_row_cnt_ >= DEFAULT_INSERT_ROW_COUNT_ONCE)
      {
        ret = batch_apply();
      }

      return ret;
    }

    int ObWriteWorker::batch_apply()
    {
      int ret = OB_SUCCESS;

      ret = client_.ups_apply(mutator_);
      stat_.add_insert_opt(1);
      stat_.add_insert_row(mutator_row_cnt_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to insert row into ups");
        display_internal_status();
      }
      else
      {
        mutator_row_cnt_ = 0;
        mutator_.reset();
      }

      return ret;
    }

    int ObWriteWorker::build_lz_rowkey()
    {
      int ret         = OB_SUCCESS;
      int64_t cur_id  = cur_rowkey_.cur_key_type_ == 0 
        ? cur_rowkey_.cur_key_id_ : cur_rowkey_.cur_creative_id_;

      rowkey_[0].set_int(cur_rowkey_.cur_unit_id_);
      rowkey_[1].set_int(cur_rowkey_.cur_date_);
      rowkey_[2].set_int(cur_rowkey_.cur_campaign_id_);
      rowkey_[3].set_int(cur_rowkey_.cur_adgroup_id_);
      rowkey_[4].set_int(cur_rowkey_.cur_key_type_);
      rowkey_[5].set_int(cur_id);
      cur_lz_rowkey_.assign(rowkey_, MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT);
      
      return ret;  
    }

    int ObWriteWorker::build_mutator()
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* lz_schema        = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name_str           = NULL;
      int32_t column_size                   = 0;
      ObString column_name;
      ObString table_name;
      ObObj obj_val;

      table_name = schema_.get_lz_name();
      lz_schema = schema_.get_lz_schema();
      if (NULL != lz_schema)
      {
        column_schema = schema_.get_schema_manager().
          get_table_schema(lz_schema->get_table_id(), column_size);
        for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
        {
          if (NULL != &column_schema[i])
          {
            ret = set_row_cells(column_schema[i], i, obj_val);
            if (OB_SUCCESS == ret)
            {
              column_name_str = column_schema[i].get_name();
              column_name.assign(const_cast<char*>(column_name_str), 
                                 static_cast<int32_t>(strlen(column_name_str)));
              ret = mutator_.insert(table_name, cur_lz_rowkey_, column_name, obj_val);
            }
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

    int ObWriteWorker::set_row_cells(
        const ObColumnSchemaV2& column_schema, 
        const int64_t column_idx, 
        ObObj& obj_val)
    {
      int ret = OB_SUCCESS;

      obj_val.reset();
      switch (column_idx)
      {
      case 0:   //shop_id
      case 1:   //customer_id
        ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_unit_id_);
        break;

      case 2:   //thedate
        ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_date_);
        break;

      case 3:   //campaign_id
        ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_campaign_id_);
        break;

      case 4:   //adgroup_id
        ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_adgroup_id_);
        break;

      case 5:   //bidword_id
        if (cur_rowkey_.cur_key_type_ == 0)
        {
          ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_key_id_);
        }
        else
        {
          ret = set_obj(obj_val, column_schema.get_type(), 0);
        }
        break;

      case 6:   //creative_id
        if (cur_rowkey_.cur_key_type_ == 0)
        {
          ret = set_obj(obj_val, column_schema.get_type(), 0);
        }
        else
        {
          ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_creative_id_);
        }
        break;

      case 7:   //shop_type
        if (cur_rowkey_.cur_key_type_ == 0)
        {
          ret = set_obj(obj_val, column_schema.get_type(), 
                        cur_rowkey_.cur_key_id_ % MAX_SHOP_TYPE);
        }
        else
        {
          ret = set_obj(obj_val, column_schema.get_type(), 
                        cur_rowkey_.cur_creative_id_ % MAX_SHOP_TYPE);
        }
        break;

      case 8:   //platform_id
        if (cur_rowkey_.cur_key_type_ == 0)
        {
          ret = set_obj(obj_val, column_schema.get_type(), 
                        cur_rowkey_.cur_key_id_ % MAX_PLATFORM_ID + 1);
        }
        else
        {
          ret = set_obj(obj_val, column_schema.get_type(), 
                        cur_rowkey_.cur_creative_id_ % MAX_PLATFORM_ID + 1);
        }
        break;

      case 9:   //pvtype_id
        if (cur_rowkey_.cur_key_type_ == 0)
        {
          ret = set_obj(obj_val, column_schema.get_type(), 
                        cur_rowkey_.cur_key_id_ % MAX_PV_TYPE);
        }
        else
        {
          ret = set_obj(obj_val, column_schema.get_type(), 
                        cur_rowkey_.cur_creative_id_ % MAX_PV_TYPE);
        }
        break;
        
      case 10:     //impression
      case 11:    //finclick
      case 12:    //finprice
      case 13:    //position_sum
      case 14:    //inner_ipv
      case 15:    //item_coll_num
      case 16:    //shop_coll_num
      case 17:    //gmv_direct_num
      case 18:    //gmv_direct_amt
      case 19:    //gmv_indirect_num
      case 20:    //gmv_indirect_amt
      case 21:    //alipay_direct_num
      case 22:    //alipay_direct_amt
      case 23:    //alipay_indirect_num
      case 24:    //alipay_indirect_amt
        if (cur_rowkey_.cur_key_type_ == 0)
        {
          ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_key_id_);
        }
        else
        {
          ret = set_obj(obj_val, column_schema.get_type(), cur_rowkey_.cur_creative_id_);
        }
        break;

      default:
        TBSYS_LOG(WARN, "more than 24 cells to store for set lz data, column_idx=%ld", 
                  column_idx);
        ret = OB_ERROR;
        break;        
      }

      return ret;
    }

    bool ObWriteWorker::is_date_changed() const
    {
      return (date_status_.wrote_row_cnt_ == 0 || (date_status_.wrote_row_cnt_ > 0 
              && date_status_.wrote_row_cnt_ == rowkey_meta_->daily_unit_count_));
    }

    bool ObWriteWorker::is_campaign_changed() const
    {
      return (campaign_status_.cur_campaign_row_cnt_ > 0 
              && campaign_status_.cur_campaign_row_cnt_ == campaign_status_.campaign_row_cnt_);
    }

    bool ObWriteWorker::is_adgroup_changed() const
    {
      return (adgroup_status_.cur_adgroup_row_cnt_ > 0 
              && adgroup_status_.cur_adgroup_row_cnt_ == adgroup_status_.adgroup_row_cnt_);
    }

    bool ObWriteWorker::is_key_type_changed() const
    {
      return (adgroup_status_.cur_adgroup_row_cnt_ > 0 
              && adgroup_status_.cur_adgroup_row_cnt_ == adgroup_status_.adgroup_row_cnt_ / 2);
    }

    bool ObWriteWorker::is_first_date() const
    {
      return (cur_rowkey_.cur_date_ <= 0);
    }

    int ObWriteWorker::do_date_change()
    {
      int ret = OB_SUCCESS;

      ret = update_prev_date();
      if (OB_SUCCESS == ret)
      {
        if (++date_status_.cur_day_cnt_ > date_status_.write_day_cnt_)
        {
          ret = OB_ITER_END;
        }
        else
        {
          date_status_.wrote_row_cnt_ = 0;
          if (cur_rowkey_.cur_date_ < rowkey_meta_->max_date_)
          {
            cur_rowkey_.reset();
            cur_rowkey_.cur_date_ = rowkey_meta_->max_date_;
          }
          ret = do_campaign_change(false);
        }
      }
      
      return ret;
    }

    int ObWriteWorker::update_rowkey_meta()
    {
      int ret = OB_SUCCESS;

      rowkey_meta_->max_date_ = cur_rowkey_.cur_date_ + 1;
      if (rowkey_meta_->max_unit_id_ == 0)
      {
        rowkey_meta_->max_unit_id_ = cur_rowkey_.cur_unit_id_ + 1;
        rowkey_meta_->max_campaign_id_ = cur_rowkey_.cur_campaign_id_ + 1;
        rowkey_meta_->max_adgroup_id_ = cur_rowkey_.cur_adgroup_id_ + 1;
        rowkey_meta_->max_key_id_ = cur_rowkey_.cur_key_id_;
        rowkey_meta_->max_creative_id_ = cur_rowkey_.cur_creative_id_;
      }
      ret = meta_.write_rowkey_meta();

      return ret;
    }

    int ObWriteWorker::update_prev_date()
    {
      int ret = OB_SUCCESS;

      if (rowkey_meta_->max_unit_id_ == 0 && rowkey_meta_->min_date_ == 0
          && cur_rowkey_.cur_unit_id_ > 0)
      {
        rowkey_meta_->min_date_ = cur_rowkey_.cur_date_;
      }

      if (cur_rowkey_.cur_campaign_id_ >= 0)
      {
        ret = update_prev_campaign();
      }     
      
      if (cur_rowkey_.cur_adgroup_id_ >= 0)
      {
        ret = update_prev_adgroup();
      }

      if (cur_rowkey_.cur_unit_id_ > 0)
      {
        ret = update_rowkey_meta();
      }

      return ret;
    }

    void ObWriteWorker::display_internal_status()
    {
      rowkey_meta_->display();
      date_status_.display();
      cur_rowkey_.display();
      campaign_status_.display();
      adgroup_status_.display();
    }

    void ObWriteWorker::calc_campaign_first_time()
    {
      int64_t prop = random() % 10000;
      
      for (int64_t i = 0; i < MAX_DIST_COUNT; ++i)
      {
        if (prop < row_dist_.dist_[i].prop_)
        {
          campaign_status_.campaign_row_cnt_ = (row_dist_.dist_[i].start_cnt_ 
                               + row_dist_.dist_[i].end_cnt_) / 2;
          campaign_status_.campaign_adgroup_cnt_ = (adgroup_dist_.dist_[i].start_cnt_ 
                                   + adgroup_dist_.dist_[i].end_cnt_) / 2;
          break;
        }
      }

      campaign_status_.cur_campaign_.campaign_id_ = cur_rowkey_.cur_campaign_id_;
      campaign_status_.cur_campaign_.start_adgroup_id_ = cur_rowkey_.cur_adgroup_id_ + 1;
      campaign_status_.cur_campaign_.start_id_ = cur_rowkey_.cur_key_id_;
    }

    void ObWriteWorker::assign_existent_campaign()
    {
      campaign_status_.campaign_row_cnt_ = (campaign_status_.cur_campaign_.end_id_ 
        - campaign_status_.cur_campaign_.start_id_) * 2;
      campaign_status_.campaign_adgroup_cnt_ = campaign_status_.cur_campaign_.end_adgroup_id_
        - campaign_status_.cur_campaign_.start_adgroup_id_;
    }

    int ObWriteWorker::do_campaign_change(bool update_prev)
    {
      int ret = OB_SUCCESS;
      
      if (update_prev && cur_rowkey_.cur_campaign_id_ >= 0)
      {
        ret = update_prev_campaign();
        if (OB_SUCCESS == ret && cur_rowkey_.cur_adgroup_id_ >= 0)
        {
          ret = update_prev_adgroup();
        }
      }

      if (OB_SUCCESS == ret)
      {
        campaign_status_.reset();
        cur_rowkey_.cur_unit_id_++;
        cur_rowkey_.cur_campaign_id_++;
  

        if (is_first_date())
        {
          //not write data once
          calc_campaign_first_time();
        }
        else
        {
          //have written data once
          ret = meta_.get_campaign(cur_rowkey_.cur_unit_id_, 
                                   cur_rowkey_.cur_campaign_id_, 
                                   campaign_status_.cur_campaign_);
          if (OB_SUCCESS == ret)
          {
            assign_existent_campaign();
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get campaign, ret=%d", ret);
            display_internal_status();
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = do_adgroup_change(false);
      }

      return ret;
    }

    int ObWriteWorker::update_prev_campaign()
    {
      int ret = OB_SUCCESS;

      campaign_status_.cur_campaign_.end_adgroup_id_ = cur_rowkey_.cur_adgroup_id_ + 1;
      campaign_status_.cur_campaign_.end_id_ = cur_rowkey_.cur_key_id_;
      if (is_first_date())
      {
        ret = meta_.store_campaign(cur_rowkey_.cur_unit_id_, cur_rowkey_.cur_campaign_id_,
                                   campaign_status_.cur_campaign_);
      }

      return ret;
    }

    void ObWriteWorker::calc_adgroup_first_time()
    {
      adgroup_status_.adgroup_row_cnt_ = campaign_status_.campaign_row_cnt_ 
                            / campaign_status_.campaign_adgroup_cnt_;

      adgroup_status_.cur_adgroup_.campaign_id_ = cur_rowkey_.cur_campaign_id_;
      adgroup_status_.cur_adgroup_.adgroup_id_ = cur_rowkey_.cur_adgroup_id_;
      adgroup_status_.cur_adgroup_.start_id_ = cur_rowkey_.cur_key_id_;
    }

    int ObWriteWorker::do_adgroup_change(bool update_prev)
    {
      int ret = OB_SUCCESS;

      if (update_prev && cur_rowkey_.cur_adgroup_id_ >= 0)
      {
        ret = update_prev_adgroup();
      }

      if (OB_SUCCESS == ret)
      {
        adgroup_status_.reset();
        cur_rowkey_.cur_adgroup_id_++;
  
        if (is_first_date())
        {
          calc_adgroup_first_time();
        }
        else if (OB_SUCCESS == ret)
        {
          ret = meta_.get_adgroup(cur_rowkey_.cur_unit_id_, 
                                  cur_rowkey_.cur_campaign_id_,
                                  cur_rowkey_.cur_adgroup_id_, 
                                  adgroup_status_.cur_adgroup_);
          if (OB_SUCCESS == ret)
          {
            adgroup_status_.adgroup_row_cnt_ = (adgroup_status_.cur_adgroup_.end_id_ 
              - adgroup_status_.cur_adgroup_.start_id_) * 2;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get adgroup, ret=%d", ret);
            display_internal_status();
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = do_key_type_change();
      }

      return ret;
    }

    int ObWriteWorker::update_prev_adgroup()
    {
      int ret = OB_SUCCESS;

      if (is_first_date())
      {
        ret = meta_.store_adgroup(cur_rowkey_.cur_unit_id_, 
                                  cur_rowkey_.cur_campaign_id_,
                                  cur_rowkey_.cur_adgroup_id_, 
                                  adgroup_status_.cur_adgroup_);
      }

      return ret;
    }

    int ObWriteWorker::do_key_type_change()
    {
      int ret = OB_SUCCESS;

      if (cur_rowkey_.cur_key_type_ == 0)
      {
        adgroup_status_.cur_adgroup_.end_id_ = cur_rowkey_.cur_key_id_;
      }
      cur_rowkey_.cur_key_type_ = !cur_rowkey_.cur_key_type_;

      return ret;
    }

    int ObWriteWorker::do_write_work()
    {
      int ret = OB_SUCCESS;
      int64_t to_write_row_cnt = 
        date_status_.write_day_cnt_ * rowkey_meta_->daily_unit_count_;
      int64_t wrote_cnt = 0;

      while (true)
      {
        ret = write_next_row();
        if (OB_ITER_END == ret)
        {
          if (wrote_cnt != to_write_row_cnt)
          {
            TBSYS_LOG(WARN, "not write expected row count, wrote_count=%ld, "
                            "nedd_write=%ld", wrote_cnt, to_write_row_cnt);
            ret = OB_ERROR;
          }
          break;
        }
        else if (OB_SUCCESS == ret)
        {
          date_status_.wrote_row_cnt_++;
          campaign_status_.cur_campaign_row_cnt_++;
          adgroup_status_.cur_adgroup_row_cnt_++;
          if (cur_rowkey_.cur_key_type_ == 0)
          {
            cur_rowkey_.cur_key_id_++;
          }
          else
          {
            cur_rowkey_.cur_creative_id_++;
          }
          wrote_cnt++;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to write row, wrote_cnt=%ld, ret=%d", 
                    wrote_cnt, ret);
          break;
        }
      }

      if (OB_SUCCESS == ret && mutator_row_cnt_ > 0)
      {
        ret = batch_apply();
      }

      return ret;
    }

    void ObWriteWorker::run(CThread* thread, void* arg)
    {
      int err = OB_SUCCESS;
      UNUSED(thread);
      UNUSED(arg);
      
      if (OB_SUCCESS == init())
      {
        while (!_stop)
        {
          err = do_write_work();
          if (OB_ITER_END == err)
          {
            break;
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to write row, err=%d", err);
            break;
          }
        }
      }
    }
  } // end namespace olapdrive
} // end namespace oceanbase
