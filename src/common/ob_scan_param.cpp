#include "ob_action_flag.h"
#include "ob_malloc.h"
#include "ob_scan_param.h"
#include "ob_rowkey_helper.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    const char * SELECT_CLAUSE_WHERE_COND_AS_CNAME_PREFIX = "__WHERE_";

    void ObScanParam::reset(void)
    {
      table_id_ = OB_INVALID_ID;
      scan_flag_.flag_ = 0;
      scan_size_ = 0;
      ObReadParam::reset();
      table_name_.assign(NULL, 0);
      range_.table_id_ = OB_INVALID_ID;
      range_.start_key_.assign(NULL, 0);
      range_.end_key_.assign(NULL, 0);
      clear_column();
      orderby_column_name_list_.clear();
      orderby_column_id_list_.clear();
      orderby_order_list_.clear();
      select_comp_column_list_.clear();
      basic_return_info_list_.clear();
      comp_return_info_list_.clear();
      if (condition_filter_.get_count() > 0)
      {
        condition_filter_.reset();
      }
      if (group_by_param_.get_aggregate_row_width() > 0)
      {
        group_by_param_.reset();
      }
      limit_offset_ = 0;
      limit_count_ = 0;
      topk_precision_  = 0;
      sharding_minimum_row_count_ = 0;
      buffer_pool_.reset();
      is_binary_rowkey_format_ = false;
    }

    // ObScanParam
    ObScanParam::ObScanParam() : table_id_(OB_INVALID_ID),
    table_name_(), range_(), /*add wenghaixing [secondary index static_index_build]*/fake_range_(), need_fake_range_(false), /*add e*/scan_size_(0), scan_flag_(), schema_manager_(NULL), is_binary_rowkey_format_(false)
    {
      limit_offset_ = 0;
      limit_count_ = 0;
      topk_precision_  = 0;
      sharding_minimum_row_count_ = 0;
      select_comp_columns_ = NULL;
      basic_column_list_.init(OB_MAX_COLUMN_NUMBER, basic_column_names_);
      basic_column_id_list_.init(OB_MAX_COLUMN_NUMBER, basic_column_ids_);
      /// select_comp_column_list_.init(sizeof(select_comp_columns_)/sizeof(ObCompositeColumn), select_comp_columns_);
      basic_return_info_list_.init(sizeof(basic_return_infos_)/sizeof(bool), basic_return_infos_);
      comp_return_info_list_.init(sizeof(comp_return_infos_)/sizeof(bool), comp_return_infos_);
      orderby_column_name_list_.init(OB_MAX_COLUMN_NUMBER, orderby_column_names_);
      orderby_column_id_list_.init(OB_MAX_COLUMN_NUMBER,orderby_column_ids_);
      orderby_order_list_.init(OB_MAX_COLUMN_NUMBER, orderby_orders_);
      deep_copy_args_ = false;
      group_by_param_.reset(deep_copy_args_);
      select_return_info_list_.add_array_helper(basic_return_info_list_);
      select_return_info_list_.add_array_helper(comp_return_info_list_);
    }

    ObScanParam::~ObScanParam()
    {
      reset();
      if (NULL != select_comp_columns_)
      {
        ob_free(select_comp_columns_);
        select_comp_columns_ = NULL;
      }
    }


    bool *ObScanParam::is_return(const int64_t c_idx)const
    {
      bool *res = NULL;
      if ((c_idx < 0) || (c_idx >= get_return_info_size()))
      {
        TBSYS_LOG(WARN,"try to access return info out of range [c_idx:%ld,return_info_size:%ld]",
          c_idx, get_return_info_size());
      }
      else if (c_idx < basic_return_info_list_.get_array_index())
      {
        res = basic_return_info_list_.at(c_idx);
      }
      else
      {
        res = comp_return_info_list_.at(c_idx - basic_return_info_list_.get_array_index());
      }
      return res;
    }

    int64_t ObScanParam::get_returned_column_num()
    {
      int64_t counter = 0;
      for (int64_t i = 0; i < get_return_info_size(); i++)
      {
        if (*is_return(i))
        {
          counter++;
        }
      }
      return counter;
    }

    int64_t ObScanParam::to_string(char *buf, int64_t buf_size) const
    {
      int64_t pos = 0;
      if (OB_SUCCESS != to_str(buf, buf_size, pos))
      {
        pos = 0;
      }
      return pos;
    }

    int ObScanParam::to_str(char *buf, int64_t buf_size, int64_t &pos)const
    {
      int err = OB_SUCCESS;
      if ((NULL == buf) || (0 >= buf_size) || (pos >= buf_size))
      {
        TBSYS_LOG(WARN,"argument error [buf:%p,buf_size:%ld, pos:%ld]", buf, buf_size, pos);
        err = OB_INVALID_ARGUMENT;
      }
      int64_t used_len = 0;
      if ((OB_SUCCESS == err) && (pos < buf_size) && ((used_len = snprintf(buf+pos,(buf_size-pos>0)?(buf_size-pos):0,"SELECT ")) > 0))
      {
        pos += used_len;
      }
      if ((OB_SUCCESS == err) && (group_by_param_.get_aggregate_row_width() > 0))
      {
        const ObArrayHelper<ObGroupByParam::ColumnInfo> &gcs = group_by_param_.get_groupby_columns();
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < gcs.get_array_index());  idx ++)
        {
          err = gcs.at(idx)->to_str(buf, buf_size, pos);
        }
        const ObArrayHelper<ObGroupByParam::ColumnInfo> &rcs = group_by_param_.get_return_columns();
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < rcs.get_array_index());  idx ++)
        {
          err = rcs.at(idx)->to_str(buf, buf_size, pos);
        }
        const ObArrayHelper<ObAggregateColumn> &acs = group_by_param_.get_aggregate_columns();
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < acs.get_array_index());  idx ++)
        {
          err = acs.at(idx)->to_str(buf, buf_size, pos);
        }
        const ObArrayHelper<ObCompositeColumn> &ccs = group_by_param_.get_composite_columns();
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < ccs.get_array_index());  idx ++)
        {
          err = ccs.at(idx)->to_str(buf, buf_size, pos);
        }
        if ((OB_SUCCESS == err) && (pos < buf_size) && ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " [BASICS:")) > 0))
        {
          pos += used_len;
        }
      }
      if ((OB_SUCCESS == err) && (OB_INVALID_ID == table_id_))
      {
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < basic_column_list_.get_array_index()); idx++)
        {
          if ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0,  "%.*s,",
            basic_column_list_.at(idx)->length(),basic_column_list_.at(idx)->ptr())) > 0)
          {
            pos += used_len;
          }
        }
      }
      else if (OB_SUCCESS  == err)
      {
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < basic_column_id_list_.get_array_index()); idx++)
        {
          if ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0,  "id:%lu,", *basic_column_id_list_.at(idx))) > 0)
          {
            pos += used_len;
          }
        }
      }
      for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < select_comp_column_list_.get_array_index()); idx++)
      {
        err = select_comp_column_list_.at(idx)->to_str(buf,buf_size,pos);
      }
      if ((group_by_param_.get_aggregate_row_width() > 0) && (OB_SUCCESS == err) && (pos < buf_size))
      {
        if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, "]")) > 0)
        {
          pos += used_len;
        }
      }
      if ((OB_SUCCESS == err) && (pos < buf_size) && ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " FROM ")) > 0))
      {
        pos += used_len;
        if ((OB_INVALID_ID == table_id_))
        {
          if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " %.*s ", table_name_.length(), table_name_.ptr())) > 0)
          {
            pos += used_len;
          }
        }
        else
        {
          if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " table_id:%lu ",  table_id_)) > 0)
          {
            pos += used_len;
          }
        }
      }
      if ((OB_SUCCESS == err) && (pos < buf_size ) && (!range_.start_key_.is_min_row() || !range_.end_key_.is_max_row()))
      {
        if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " WHERE ")) > 0)
        {
          pos += used_len;
        }
        if (!range_.start_key_.is_min_row())
        {
          used_len = range_.start_key_.to_string(buf + pos, buf_size - pos);
          pos += used_len;
        }

        if (!range_.end_key_.is_max_row())
        {
          used_len = range_.end_key_.to_string(buf + pos, buf_size - pos);
          pos += used_len;
        }
      }
      if ((group_by_param_.get_aggregate_row_width() > 0) &&(OB_SUCCESS == err) && (pos < buf_size))
      {
        if ((OB_SUCCESS == err) && (pos < buf_size) && ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0,  " GROUP BY ")) > 0))
        {
          pos += used_len;
        }
        const ObArrayHelper<ObGroupByParam::ColumnInfo> &gcs = group_by_param_.get_groupby_columns();
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < gcs.get_array_index());  idx ++)
        {
          err = gcs.at(idx)->to_str(buf, buf_size, pos);
        }
      }
      if ((OB_SUCCESS == err) && (pos < buf_size) && (orderby_order_list_.get_array_index() > 0))
      {
        if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " ORDER BY ")) > 0)
        {
          pos += used_len;
        }
        for (int64_t idx = 0; (OB_SUCCESS == err) && (pos < buf_size) && (idx < orderby_order_list_.get_array_index()); idx ++)
        {
          static const char * order_expr[] = {"","ASC","DESC"};
          if (OB_INVALID_ID == table_id_)
          {
            if ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "%.*s %s,", orderby_column_name_list_.at(idx)->length(),
              orderby_column_name_list_.at(idx)->ptr(), order_expr[*orderby_order_list_.at(idx)])) > 0)
            {
              pos += used_len;
            }
          }
          else
          {
            if ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "idx:%ld %s,", *orderby_column_id_list_.at(idx),
              order_expr[*orderby_order_list_.at(idx)])) > 0)
            {
              pos += used_len;
            }
          }
        }
      }

      if ((OB_SUCCESS == err) && (pos < buf_size) && ((limit_offset_ > 0) || (limit_count_ > 0)))
      {
        if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " LIMIT %ld,%ld", limit_offset_, limit_count_)) > 0)
        {
          pos += used_len;
        }
      }
      if ((OB_SUCCESS == err) && (pos < buf_size) && (0 != topk_precision_))
      {
        if ((used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, " PRECISION %.4f,%ld", topk_precision_, sharding_minimum_row_count_)) > 0)
        {
          pos += used_len;
        }
      }
      return err;
    }

    int  ObScanParam::get_select_column_name(const int64_t column_idx, ObString & column_name)const
    {
      int err = OB_SUCCESS;
      int counter = 0;
      int i = 0;
      for (i = 0; i < get_return_info_size(); i++)
      {
        if ((*is_return(i)) && (counter++ == column_idx))
        {
          break;
        }
      }

      if (OB_SUCCESS == err)
      {
        if (i < basic_column_list_.get_array_index())
        {
          column_name = *basic_column_list_.at(i);
        }
        else
        {
          column_name = select_comp_column_list_.at(i - basic_column_list_.get_array_index())->get_as_column_name();
        }
      }
      return err;
    }

    int ObScanParam::safe_copy(const ObScanParam & other)
    {
      int err = OB_SUCCESS;
      ObCompositeColumn * old_select_comp_columns = select_comp_columns_;
      deep_copy_args_ = other.deep_copy_args_;
      table_id_ = other.table_id_;
      table_name_ = other.table_name_;
      range_ = other.range_;
      scan_size_ = other.scan_size_;
      scan_flag_ = other.scan_flag_;
      set_is_result_cached(other.get_is_result_cached());
      set_version_range(other.get_version_range());
      memcpy(basic_column_names_, other.basic_column_names_, sizeof(basic_column_names_));
      basic_column_list_.init(OB_MAX_COLUMN_NUMBER, basic_column_names_, other.basic_column_list_.get_array_index());

      is_binary_rowkey_format_ = other.is_binary_rowkey_format_;
      schema_manager_ = other.schema_manager_;

      memcpy(basic_column_ids_, other.basic_column_ids_, sizeof(basic_column_ids_));
      basic_column_id_list_.init(OB_MAX_COLUMN_NUMBER, basic_column_ids_, other.basic_column_id_list_.get_array_index());

      memcpy(basic_return_infos_, other.basic_return_infos_, sizeof(basic_return_infos_));
      basic_return_info_list_.init(sizeof(basic_return_infos_)/sizeof(bool), basic_return_infos_,
        other.basic_return_info_list_.get_array_index());

      memcpy(comp_return_infos_, other.comp_return_infos_, sizeof(comp_return_infos_));
      comp_return_info_list_.init(sizeof(comp_return_infos_)/sizeof(bool), comp_return_infos_,
        other.comp_return_info_list_.get_array_index());

      limit_offset_ = other.limit_offset_;
      limit_count_ = other.limit_count_;
      topk_precision_  = other.topk_precision_;
      sharding_minimum_row_count_ = other.sharding_minimum_row_count_;

      memcpy(orderby_column_names_, other.orderby_column_names_, sizeof(orderby_column_names_));
      orderby_column_name_list_.init(OB_MAX_COLUMN_NUMBER, orderby_column_names_, other.orderby_column_name_list_.get_array_index());

      memcpy(orderby_column_ids_, other.orderby_column_ids_, sizeof(orderby_column_ids_));
      orderby_column_id_list_.init(OB_MAX_COLUMN_NUMBER,orderby_column_ids_, other.orderby_column_id_list_.get_array_index());

      memcpy(orderby_orders_, other.orderby_orders_, sizeof(orderby_orders_));
      orderby_order_list_.init(OB_MAX_COLUMN_NUMBER, orderby_orders_, other.orderby_order_list_.get_array_index());

      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = condition_filter_.safe_copy(other.condition_filter_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy condition filter");
      }

      if ((NULL == old_select_comp_columns) && (NULL != other.select_comp_columns_))
      {
        if (OB_SUCCESS == (err = malloc_composite_columns()))
        {
          old_select_comp_columns = select_comp_columns_;
        }
      }
      if (OB_SUCCESS == err)
      {
        select_comp_columns_ = old_select_comp_columns;
        select_comp_column_list_.init(OB_MAX_COLUMN_NUMBER, old_select_comp_columns,
          other.select_comp_column_list_.get_array_index());
        if (NULL != other.select_comp_columns_)
        {
          memcpy(old_select_comp_columns, other.select_comp_columns_, sizeof(ObCompositeColumn)*OB_MAX_COLUMN_NUMBER);

        }
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = group_by_param_.safe_copy(other.group_by_param_))))
      {
        TBSYS_LOG(WARN,"fail to safe copy groupby param [err:%d]", err);
      }
      return err;
    }


    int ObScanParam::set_range(const ObNewRange& range)
    {
      int err = OB_SUCCESS;
      range_ = range;
      table_id_ = range_.table_id_;
      if (deep_copy_args_)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.start_key_,&(range_.start_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.start_key_ to local buffer [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.end_key_,&(range_.end_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.end_key_ to local buffer [err:%d]", err);
        }
      }
      return err;
    }
    //add wenghaixing [secondary index static_index_build.cs_scan]20150326
    int ObScanParam::set_fake_range(const ObNewRange &fake_range)
    {
      int err = OB_SUCCESS;
      fake_range_ = fake_range;
      //table_id_ = fake_range_.table_id_;
      if (deep_copy_args_)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(fake_range.start_key_,&(fake_range_.start_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.start_key_ to local buffer [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(fake_range.end_key_,&(fake_range_.end_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.end_key_ to local buffer [err:%d]", err);
        }
      }
      return err;
    }
    void ObScanParam::set_copy_args(bool arg)
    {
      deep_copy_args_ = arg;
    }
    //add e

    int ObScanParam::set(const uint64_t& table_id, const ObString& table_name, const ObNewRange& range, bool deep_copy_args)
    {
      int err = OB_SUCCESS;
      deep_copy_args_ = deep_copy_args;
      table_name_ = table_name;
      group_by_param_.reset(deep_copy_args_);
      if (OB_SUCCESS != (err = set_range(range)))
      {
        TBSYS_LOG(WARN,"fail to set range [err:%d]", err);
      }
      else if (deep_copy_args_)
      {
        if (OB_SUCCESS != (err = buffer_pool_.write_string(table_name, &table_name_)))
        {
          TBSYS_LOG(WARN,"fail to copy table name to local buffer [err:%d]", err);
        }
      }

      range_.table_id_ = table_id;
      table_id_ = table_id;
      return err;
    }

    int ObScanParam::add_orderby_column(const ObString & column_name, Order order)
    {
      int err = OB_SUCCESS;
      if (orderby_column_id_list_.get_array_index() > 0)
      {
        TBSYS_LOG(WARN,"use only column id or column name, not both");
        err = OB_INVALID_ARGUMENT;
      }
      ObString stored_column_name = column_name;
      if ((OB_SUCCESS == err)
        && deep_copy_args_
        && (OB_SUCCESS != (err = buffer_pool_.write_string(column_name,&stored_column_name))))
      {
        TBSYS_LOG(WARN,"fail to copy column_name to local buffer [err:%d]", err);
      }
      if (OB_SUCCESS == err)
      {
        if (!orderby_column_name_list_.push_back(stored_column_name))
        {
          err = OB_ARRAY_OUT_OF_RANGE;
        }
        else
        {
          orderby_order_list_.push_back(order);
        }
      }
      return err;
    }

    int ObScanParam::add_orderby_column(const int64_t column_idx, Order order)
    {
      int err = OB_SUCCESS;
      if (orderby_column_name_list_.get_array_index() > 0)
      {
        TBSYS_LOG(WARN,"use only column id or column name, not both");
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        if (!orderby_column_id_list_.push_back(column_idx))
        {
          err = OB_ARRAY_OUT_OF_RANGE;
        }
        else
        {
          orderby_order_list_.push_back(order);
        }
      }
      return err;
    }

    int64_t ObScanParam::get_orderby_column_size()const
    {
      return orderby_order_list_.get_array_index();
    }


    void ObScanParam::get_orderby_column(ObString const *& names, uint8_t const *& orders,
      int64_t &column_size)const
    {
      column_size = get_orderby_column_size();
      orders = orderby_orders_;
      if (0 == orderby_column_name_list_.get_array_index())
      {
        names = NULL;
      }
      else
      {
        names = orderby_column_names_;
      }
    }

    void ObScanParam::get_orderby_column(int64_t const* & column_idxs, uint8_t const * & orders,
      int64_t &column_size)const
    {
      column_size = get_orderby_column_size();
      orders = orderby_orders_;
      if (0 == orderby_column_id_list_.get_array_index())
      {
        column_idxs = NULL;
      }
      else
      {
        column_idxs = orderby_column_ids_;
      }
    }

    void ObScanParam::set_all_column_return()
    {
      for (int64_t i = 0; i < basic_return_info_list_.get_array_index(); i++)
      {
        basic_return_infos_[i] = true;
      }
      for (int64_t i = 0; i < comp_return_info_list_.get_array_index(); i++)
      {
        comp_return_infos_[i] = true;
      }
      group_by_param_.set_all_column_return();
    }

    int ObScanParam::set_limit_info(const int64_t offset, int64_t count)
    {
      int err = OB_SUCCESS;
      if (offset < 0 || count < 0)
      {
        TBSYS_LOG(WARN,"param error [offset:%ld,count:%ld]", offset, count);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        limit_offset_ = offset;
        limit_count_ = count;
      }
      return err;
    }

    void ObScanParam::get_limit_info(int64_t &offset, int64_t &count) const
    {
      offset = limit_offset_;
      count = limit_count_;
    }

    int ObScanParam::set_topk_precision(const int64_t sharding_minimum_row_count, const double precision)
    {
      int err = OB_SUCCESS;
      if (sharding_minimum_row_count < 0 || precision < 0)
      {
        TBSYS_LOG(WARN,"param error [sharding_minimum_row_count:%ld,precision:%f]", sharding_minimum_row_count, precision);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        sharding_minimum_row_count_ = sharding_minimum_row_count;
        topk_precision_ = precision;
        if (topk_precision_ - 1 >= 0)
        {
          sharding_minimum_row_count_ = 0;
          topk_precision_ = 0;
        }
      }
      return err;
    }

    void ObScanParam::get_topk_precision(int64_t &sharding_minimum_row_count, double &precision) const
    {
      sharding_minimum_row_count = sharding_minimum_row_count_;
      precision = topk_precision_;
    }

    int ObScanParam::add_column(const ObString& column_name, bool is_return)
    {
      int rc = OB_SUCCESS;
      if (basic_column_id_list_.get_array_index() > 0)
      {
        rc = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"using column id or column name, not both");
      }

      if (OB_SUCCESS == rc && !basic_column_list_.push_back(column_name))
      {
        rc = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN,"basic column is full");
      }
      if (OB_SUCCESS == rc && !basic_return_info_list_.push_back(is_return))
      {
        rc = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN,"select return info is full");
      }
      if ((OB_SUCCESS == rc)
        && deep_copy_args_
        && (OB_SUCCESS !=
        (rc = buffer_pool_.write_string(column_name, basic_column_names_ + basic_column_list_.get_array_index()-1))))
      {
        TBSYS_LOG(WARN,"fail to copy column name to local buffer [err:%d]", rc);
      }
      return rc;
    }


    int ObScanParam::add_column(const uint64_t& column_id, bool is_return)
    {
      int rc = OB_SUCCESS;
      if (basic_column_list_.get_array_index() > 0)
      {
        rc = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"using column id or column name, not both");
      }

      if (OB_SUCCESS == rc && !basic_column_id_list_.push_back(column_id))
      {
        rc = OB_ARRAY_OUT_OF_RANGE;
      }

      if (OB_SUCCESS == rc && !basic_return_info_list_.push_back(is_return))
      {
        rc = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN,"select return info is full");
      }

      return rc;
    }


    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    //int ObScanParam::add_column(const ObString & expr, const ObString & as_name, bool is_return)
    int ObScanParam::add_column(const ObString & expr, const ObString & as_name, bool is_return, bool is_expire_info)
    {
      int err = OB_SUCCESS;
      if (NULL == select_comp_columns_)
      {
        err = malloc_composite_columns();
      }
      ObString stored_expr = expr;
      ObString stored_as_name = as_name;
      ObCompositeColumn comp_column;
      if ((OB_SUCCESS == err ) && deep_copy_args_
        && (OB_SUCCESS != (err = buffer_pool_.write_string(expr,&stored_expr))))
      {
        TBSYS_LOG(WARN,"fail to copy expr to local buffer [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && deep_copy_args_
        && (OB_SUCCESS != (err = buffer_pool_.write_string(as_name, &stored_as_name))))
      {
        TBSYS_LOG(WARN,"fail to copy as_name to local buffer [err:%d]", err);
      }
      //if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = comp_column.set_expression(stored_expr, stored_as_name))))
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = comp_column.set_expression(stored_expr, stored_as_name, is_expire_info))))
      {
        TBSYS_LOG(WARN,"fail to set expression [err:%d,expr:%.*s, as_name:%.*s]", err,
          stored_expr.length(), stored_expr.ptr(), stored_as_name.length(), stored_as_name.ptr());
      }
      if ((OB_SUCCESS == err) && (!select_comp_column_list_.push_back(comp_column)))
      {
        TBSYS_LOG(WARN,"select composite column list is full");
        err = OB_ARRAY_OUT_OF_RANGE;
      }

      if ((OB_SUCCESS == err) && (!comp_return_info_list_.push_back(is_return)))
      {
        TBSYS_LOG(WARN,"select return info list is full");
        err = OB_ARRAY_OUT_OF_RANGE;
      }
      return err;
    }
    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

    int ObScanParam::add_column(const ObObj *expr, bool is_return)
    {
      int err = OB_SUCCESS;
      if (NULL == select_comp_columns_)
      {
        err = malloc_composite_columns();
      }
      ObCompositeColumn comp_column;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = comp_column.set_expression(expr, buffer_pool_))))
      {
        TBSYS_LOG(WARN,"fail to set expression [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (!select_comp_column_list_.push_back(comp_column)))
      {
        TBSYS_LOG(WARN,"select composite column list is full");
        err = OB_ARRAY_OUT_OF_RANGE;
      }

      if ((OB_SUCCESS == err) && (!comp_return_info_list_.push_back(is_return)))
      {
        TBSYS_LOG(WARN,"select return info list is full");
        err = OB_ARRAY_OUT_OF_RANGE;
      }
      return err;
    }

    int ObScanParam::add_where_cond(const ObString & expr, bool is_expire_cond/* = false*/)
    {
      int err = OB_SUCCESS;
      static const int32_t max_filter_column_name = 128;
      char filter_column_name[max_filter_column_name]="";
      int32_t filter_column_name_len = 0;
      ObObj false_obj;
      ObString filter_column_name_str;
      ObString as_name;
      ObString stored_expr = expr;
      false_obj.set_bool(false);
      filter_column_name_len = snprintf(filter_column_name,sizeof(filter_column_name),
        "%s%ld",SELECT_CLAUSE_WHERE_COND_AS_CNAME_PREFIX, get_return_info_size());
      filter_column_name_str.assign(filter_column_name,filter_column_name_len);
      if ((OB_SUCCESS == err) && ((expr.length() == 0) || (expr.ptr() == NULL)))
      {
        TBSYS_LOG(WARN,"argumen error, empty expr [expr.length():%d,expr.ptr():%p]", expr.length(), expr.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(filter_column_name_str, &as_name))))
      {
        TBSYS_LOG(WARN,"fail to copy as column name [err:%d]", err);
      }

      if ((OB_SUCCESS == err) && deep_copy_args_ && (OB_SUCCESS != (err = buffer_pool_.write_string(expr, &stored_expr))))
      {
        TBSYS_LOG(WARN,"fail to copy expr [err:%d]", err);
      }

      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //if ((OB_SUCCESS == err ) && (OB_SUCCESS != (err = add_column(stored_expr,as_name,false))))
      if ((OB_SUCCESS == err ) && (OB_SUCCESS != (err = add_column(stored_expr,as_name,false, is_expire_cond))))
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
      {
        TBSYS_LOG(WARN,"fail to add composite column [err:%d,expr:%.*s]", err, stored_expr.length(), stored_expr.ptr());
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = add_where_cond(as_name, is_expire_cond ? EQ : NE, false_obj))))
      {
        TBSYS_LOG(WARN,"fail to add condition [err:%d]", err);
      }
      return err;
    }

    void ObScanParam::clear_column(void)
    {
      basic_column_id_list_.clear();
      basic_column_list_.clear();
      select_comp_column_list_.clear();
      basic_return_info_list_.clear();
      comp_return_info_list_.clear();
    }

    int ObScanParam::add_where_cond(const ObString & column_name, const ObLogicOperator & cond_op,
      const ObObj & cond_value)
    {
      int err = OB_SUCCESS;
      ObString stored_column_name = column_name;
      ObObj stored_cond_value = cond_value;
      if (deep_copy_args_)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(column_name,&stored_column_name))))
        {
          TBSYS_LOG(WARN,"fail to copy colum_name to local buffer [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_obj(cond_value,&stored_cond_value))))
        {
          TBSYS_LOG(WARN,"fail to copy cond_value to local buffer [err:%d]", err);
        }
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = condition_filter_.add_cond(stored_column_name, cond_op, stored_cond_value))))
      {
        TBSYS_LOG(WARN,"fail to add condition [err:%d]", err);
      }
      return err;
    }

    const ObSimpleFilter & ObScanParam::get_filter_info(void)const
    {
      return condition_filter_;
    }

    ObSimpleFilter & ObScanParam::get_filter_info(void)
    {
      return condition_filter_;
    }


    const ObGroupByParam &ObScanParam::get_group_by_param()const
    {
      return group_by_param_;
    }

    ObGroupByParam &ObScanParam::get_group_by_param()
    {
      return group_by_param_;
    }



    ObScanner* ObScanParam::get_location_info() const
    {
      return tablet_location_scanner_;
    }

    int ObScanParam::set_location_info(const ObScanner &obscanner)
    {
      int ret = OB_SUCCESS;
      tablet_location_scanner_ = const_cast<ObScanner*>(&obscanner);
      return ret;
    }

    // BASIC_PARAM_FIELD
    int ObScanParam::serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      // read param info
      if (OB_SUCCESS == ret)
      {
        ret = ObReadParam::serialize(buf, buf_len, pos);
      }


      // table name or id
      if (OB_SUCCESS == ret)
      {
        if (table_name_.length() > 0)
        {
          if (OB_INVALID_ID == table_id_)
          {
            obj.set_varchar(table_name_);
          }
          else
          {
            ret = OB_ERROR;
          }
        }
        else
        {
          if (OB_INVALID_ID != table_id_)
          {
            obj.set_int(table_id_);
          }
          else
          {
            ret = OB_ERROR;
          }
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Both table_name and table_id are set");
        }
        else
        {
          ret = obj.serialize(buf, buf_len, pos);
        }
      }
      //add wenghaixing [secondary index static_index_build.cs_scan]20150326
      //将fake_range序列化到buffer里面
      if (OB_SUCCESS == ret)
      {
        int32_t bl = need_fake_range_ ? 1 : 0;
        ret = serialization::encode_i32(buf, buf_len, pos, bl);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf, buf_len, pos, (int64_t)fake_range_.table_id_);
      }
      if (OB_SUCCESS == ret)
      {
        obj.set_int(fake_range_.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = set_rowkey_obj_array(buf, buf_len, pos,
              fake_range_.start_key_.get_obj_ptr(), fake_range_.start_key_.get_obj_cnt());
        }
       // TBSYS_LOG(ERROR,"test::whx fake range = %s, range = [%s], ret[%d]",to_cstring(fake_range_), to_cstring(range_), ret);
        if (OB_SUCCESS == ret)
        {
          ret = set_rowkey_obj_array(buf, buf_len, pos,
              fake_range_.end_key_.get_obj_ptr(), fake_range_.end_key_.get_obj_cnt());
        }
        //obj.reset();
      }
      //add e
      // scan range
      if (OB_SUCCESS == ret)
      {
        obj.set_int(range_.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = set_rowkey_obj_array(buf, buf_len, pos,
              range_.start_key_.get_obj_ptr(), range_.start_key_.get_obj_cnt());
        }
        //TBSYS_LOG(ERROR,"test::whx phase 2 fake range  = %s, range = [%s], ret[%d]",to_cstring(fake_range_), to_cstring(range_), ret);
        if (OB_SUCCESS == ret)
        {
          ret = set_rowkey_obj_array(buf, buf_len, pos,
              range_.end_key_.get_obj_ptr(), range_.end_key_.get_obj_cnt());
        }
      }

      // scan direction
      if (OB_SUCCESS == ret)
      {
        obj.set_int(scan_flag_.flag_);
        ret = obj.serialize(buf, buf_len, pos);
        // scan size
        if (OB_SUCCESS == ret)
        {
          obj.set_int(scan_size_);
          ret = obj.serialize(buf, buf_len, pos);
        }
      }

      //add zhujun [transaction read uncommit]2016/3/24
      //trans_id
      if (OB_SUCCESS == ret)
      {
          //TBSYS_LOG(INFO,"ObScanParam:serialize transaction id:%s",to_cstring(trans_id_));
          ret = trans_id_.serialize(buf,buf_len,pos);
      }
      //add:e

      return ret;
    }

    int ObScanParam::deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int64_t int_value = 0;
      int8_t border_flag = 0;
      ObRowkeyInfo rowkey_info;
      int ret = ObReadParam::deserialize(buf, data_len, pos);
      // table name or table id
      ObObj obj;
      ObString str_value;
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          if (ObIntType == obj.get_type())
          {
            ret = obj.get_int(int_value);
            if (OB_SUCCESS == ret)
            {
              table_id_ = int_value;
              range_.table_id_ = int_value;
            }
          }
          else
          {
            ret = obj.get_varchar(str_value);
            if (OB_SUCCESS == ret)
            {
              table_name_ = str_value;
              range_.table_id_ = 0;
            }
          }
        }
      }

      if (OB_SUCCESS == ret && NULL != schema_manager_)
      {
        get_rowkey_info_from_sm(schema_manager_, range_.table_id_, table_name_, rowkey_info);
      }
      //add wenghaixing [secondary index static_index_build]20150326
      //首先反串行化need_fake_range_
      int32_t bl = 0;
      int64_t tid = OB_INVALID_ID;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i32(buf, data_len, pos, &bl);
      }
      if (OB_SUCCESS == ret)
      {
        need_fake_range_ = bl == 1 ? true : false;
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, pos, &tid);
      }
      if (OB_SUCCESS == ret)
      {
        fake_range_.table_id_ = (uint64_t)tid;
      }
      //其次反串行化fake_range
      if (OB_SUCCESS == ret)
      {
        // border flag
        if (OB_SUCCESS == ret)
        {
          ret = obj.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            ret = obj.get_int(int_value);
            if (OB_SUCCESS == ret)
            {
              border_flag = static_cast<int8_t>(int_value);
              fake_range_.border_flag_.set_data(static_cast<int8_t>(int_value));
            }
          }
        }

        // start key
        if (OB_SUCCESS == ret)
        {
          int_value = OB_MAX_COLUMN_NUMBER;
          ret = get_rowkey_compatible(buf, data_len, pos,
              rowkey_info, fake_start_rowkey_obj_array_, int_value, is_binary_rowkey_format_);
          if (OB_SUCCESS == ret)
          {
            fake_range_.start_key_.assign(fake_start_rowkey_obj_array_, int_value);
          }
        }

        // end key
        if (OB_SUCCESS == ret)
        {
          int_value = OB_MAX_COLUMN_NUMBER;
          ret = get_rowkey_compatible(buf, data_len, pos,
              rowkey_info, fake_end_rowkey_obj_array_, int_value, is_binary_rowkey_format_);
          if (OB_SUCCESS == ret)
          {
            fake_range_.end_key_.assign(fake_end_rowkey_obj_array_, int_value);
          }
        }

        // compatible: old client may send request with min/max borderflag info.
        if (OB_SUCCESS == ret)
        {
          if (ObBorderFlag::MIN_VALUE & border_flag)
          {
            fake_range_.start_key_.set_min_row();
          }
          if (ObBorderFlag::MAX_VALUE& border_flag)
          {
            fake_range_.end_key_.set_max_row();
          }
        }
      }
      //add e

      // scan range
      if (OB_SUCCESS == ret)
      {
        // border flag
        if (OB_SUCCESS == ret)
        {
          ret = obj.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            ret = obj.get_int(int_value);
            if (OB_SUCCESS == ret)
            {
              border_flag = static_cast<int8_t>(int_value);
              range_.border_flag_.set_data(static_cast<int8_t>(int_value));
            }
          }
        }

        // start key
        if (OB_SUCCESS == ret)
        {
          int_value = OB_MAX_COLUMN_NUMBER;
          ret = get_rowkey_compatible(buf, data_len, pos,
              rowkey_info, start_rowkey_obj_array_, int_value, is_binary_rowkey_format_);
          if (OB_SUCCESS == ret)
          {
            range_.start_key_.assign(start_rowkey_obj_array_, int_value);
          }
        }

        // end key
        if (OB_SUCCESS == ret)
        {
          int_value = OB_MAX_COLUMN_NUMBER;
          ret = get_rowkey_compatible(buf, data_len, pos,
              rowkey_info, end_rowkey_obj_array_, int_value, is_binary_rowkey_format_);
          if (OB_SUCCESS == ret)
          {
            range_.end_key_.assign(end_rowkey_obj_array_, int_value);
          }
        }

        // compatible: old client may send request with min/max borderflag info.
        if (OB_SUCCESS == ret)
        {
          if (ObBorderFlag::MIN_VALUE & border_flag)
          {
            range_.start_key_.set_min_row();
          }
          if (ObBorderFlag::MAX_VALUE& border_flag)
          {
            range_.end_key_.set_max_row();
          }
        }
      }
      // scan direction
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(scan_flag_.flag_);
        }
      }
      // scan size
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            scan_size_ = int_value;
          }
        }
      }

      //add zhujun [transaction read uncommit]//2016/3/24
      // trans_id
      if (OB_SUCCESS == ret)
      {
        ObTransID temp;
        ret = temp.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
            trans_id_ = temp;
            //add huangcc [fix transaction read uncommit bug]2016/11/21
            if (trans_id_.is_valid())
            {
                  is_read_master_ = true;
            }
            //add end
        }
        //TBSYS_LOG(INFO,"ObScanParam::deserialize transaction id:%s",to_cstring(temp));
      }
      //add:e
      return ret;
    }

    int64_t ObScanParam::get_basic_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // BASIC_PARAM_FIELD
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      total_size += obj.get_serialize_size();

      /// READ PARAM
      total_size += ObReadParam::get_serialize_size();

      // table name id
      if (table_name_.length() > 0)
      {
        obj.set_varchar(table_name_);
      }
      else
      {
        obj.set_int(table_id_);
      }
      total_size += obj.get_serialize_size();
      ///add wenghaixing [secondary index static_index_build]20150326
      //添加了两个int32，和一个range的序列化大小
      total_size += sizeof(int32_t);
      total_size += sizeof(int64_t);
      // scan range
      obj.set_int(range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();

      // start_key_
      total_size += get_rowkey_obj_array_size(
          fake_range_.start_key_.get_obj_ptr(), fake_range_.start_key_.get_obj_cnt());

      // end_key_
      total_size += get_rowkey_obj_array_size(
          fake_range_.end_key_.get_obj_ptr(), fake_range_.end_key_.get_obj_cnt());
      ///add e
      // scan range
      obj.set_int(range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();

      // start_key_
      total_size += get_rowkey_obj_array_size(
          range_.start_key_.get_obj_ptr(), range_.start_key_.get_obj_cnt());

      // end_key_
      total_size += get_rowkey_obj_array_size(
          range_.end_key_.get_obj_ptr(), range_.end_key_.get_obj_cnt());

      // scan sequence
      obj.set_int(scan_flag_.flag_);
      total_size += obj.get_serialize_size();
      // scan size
      obj.set_int(scan_size_);
      total_size += obj.get_serialize_size();

      //add zhujun [transaction read uncommit]2016/3/24
      //trans_id
      total_size += trans_id_.get_serialize_size();
      return total_size;
    }

    // COLUMN_PARAM_FIELD
    int ObScanParam::serialize_column_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::COLUMN_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS == ret)
      {
        int64_t column_size = basic_column_list_.get_array_index();
        int64_t column_id_size = basic_column_id_list_.get_array_index();
        if ((column_size > 0) && (column_id_size > 0))
        {
          TBSYS_LOG(ERROR, "check column size failed");
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          for (int64_t i = 0; i < column_size; ++i)
          {
            obj.set_varchar(basic_column_names_[i]);
            ret = obj.serialize(buf, buf_len, pos);
            if (ret != OB_SUCCESS)
            {
              break;
            }
          }

          for (int64_t i = 0; i < column_id_size; ++i)
          {
            obj.set_int(basic_column_ids_[i]);
            ret = obj.serialize(buf, buf_len, pos);
            if (ret != OB_SUCCESS)
            {
              break;
            }
          }
        }
      }
      return ret;
    }

    int ObScanParam::deserialize_column_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ObString str_value;
      ObObj obj;
      int64_t old_pos = pos;
      while ((OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
        && (ObExtendType != obj.get_type()))
      {
        old_pos = pos;
        if (ObIntType == obj.get_type())
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            if (!basic_column_id_list_.push_back(int_value))
            {
              ret = OB_ERROR;
            }
          }
        }
        else
        {
          ret = obj.get_varchar(str_value);
          if (OB_SUCCESS == ret)
          {
            if (!basic_column_list_.push_back(str_value))
            {
              ret = OB_ERROR;
            }
          }
        }
        if (OB_SUCCESS != ret)
        {
          break;
        }
      }
      // modify last pos for the last ext_type obj
      pos = old_pos;
      return ret;
    }

    int64_t ObScanParam::get_column_param_serialize_size(void) const
    {
      ObObj obj;
      int64_t total_size = 0;
      // COLUMN_PARAM_FIELD
      obj.set_ext(ObActionFlag::COLUMN_PARAM_FIELD);
      total_size += obj.get_serialize_size();
      int64_t column_size = basic_column_list_.get_array_index();
      int64_t column_id_size = basic_column_id_list_.get_array_index();
      // scan name
      for (int64_t i=0; i<column_size; ++i)
      {
        obj.set_varchar(basic_column_names_[i]);
        total_size += obj.get_serialize_size();
      }
      // scan id
      for (int64_t i = 0; i < column_id_size; ++i)
      {
        obj.set_int(basic_column_ids_[i]);
        total_size += obj.get_serialize_size();
      }
      return total_size;
    }

    // SORT_PARAM_FIELD
    int ObScanParam::serialize_sort_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::SORT_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      int64_t orderby_column_size = orderby_column_name_list_.get_array_index();
      int64_t orderby_column_id_size = orderby_column_id_list_.get_array_index();
      if (OB_SUCCESS == ret)
      {
        if ((orderby_column_size > 0) && (orderby_column_id_size > 0))
        {
          TBSYS_LOG(ERROR, "check column size failed");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < orderby_column_size; i++)
        {
          obj.set_varchar(orderby_column_names_[i]);
          ret = obj.serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
          obj.set_int(orderby_orders_[i]);
          ret = obj.serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
        }

        for (int64_t i = 0; i < orderby_column_id_size; i++)
        {
          obj.set_int(orderby_column_ids_[i]);
          ret = obj.serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
          obj.set_int(orderby_orders_[i]);
          ret = obj.serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
        }
      }
      return ret;
    }

    int ObScanParam::deserialize_sort_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ObString str_value;
      int64_t old_pos = pos;
      ObObj obj;
      while ((OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
        && (ObExtendType != obj.get_type()))
      {
        old_pos = pos;
        // column
        if (ObIntType == obj.get_type())
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            if (!orderby_column_id_list_.push_back(int_value))
            {
              ret = OB_ERROR;
              break;
            }
          }
        }
        else
        {
          ret = obj.get_varchar(str_value);
          if (!orderby_column_name_list_.push_back(str_value))
          {
            ret = OB_ERROR;
            break;
          }
        }

        // orderby
        if (OB_SUCCESS == ret)
        {
          ret = obj.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            ret = obj.get_int(int_value);
            if (OB_SUCCESS == ret)
            {
              if (!orderby_order_list_.push_back(static_cast<uint8_t>(int_value)))
              {
                ret = OB_ERROR;
                break;
              }
            }
          }
        }
        if (OB_SUCCESS != ret)
        {
          break;
        }
      }
      // modify last pos for the last ext_type obj
      pos = old_pos;
      return ret;
    }

    int64_t ObScanParam::get_sort_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      /// SORT PARAM FIELD
      obj.set_ext(ObActionFlag::SORT_PARAM_FIELD);
      total_size += obj.get_serialize_size();

      // not check again
      int64_t orderby_order_size = orderby_order_list_.get_array_index();
      int64_t orderby_column_size = orderby_column_name_list_.get_array_index();
      int64_t orderby_column_id_size = orderby_column_id_list_.get_array_index();
      // name
      for (int64_t i = 0; i < orderby_column_size; i++)
      {
        obj.set_varchar(orderby_column_names_[i]);
        total_size += obj.get_serialize_size();
      }
      // id
      for (int64_t i = 0; i < orderby_column_id_size; i++)
      {
        obj.set_int(orderby_column_ids_[i]);
        total_size += obj.get_serialize_size();
      }

      // orderby
      for (int64_t i = 0; i < orderby_order_size; ++i)
      {
        obj.set_int(orderby_orders_[i]);
        total_size += obj.get_serialize_size();
      }
      return total_size;
    }

    int ObScanParam::serialize_limit_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::LIMIT_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS == ret)
      {
        obj.set_int(limit_offset_);
        ret = obj.serialize(buf, buf_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        obj.set_int(limit_count_);
        ret = obj.serialize(buf, buf_len, pos);
      }


      return ret;
    }

    int ObScanParam::serialize_topk_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::TOPK_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);

      if (OB_SUCCESS == ret)
      {
        obj.set_int(sharding_minimum_row_count_);
        ret = obj.serialize(buf, buf_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        obj.set_double(topk_precision_);
        ret = obj.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    int ObScanParam::deserialize_limit_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ObString str_value;
      ObObj obj;
      // offset
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          limit_offset_ = int_value;
        }
      }
      // count
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            limit_count_ = int_value;
          }
        }
      }

      return ret;
    }

    int ObScanParam::deserialize_topk_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ObString str_value;
      // offset
      ObObj obj;
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          sharding_minimum_row_count_ = int_value;
        }
      }
      // count
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          double double_value = 0.0f;
          ret = obj.get_double(double_value);
          if (OB_SUCCESS == ret)
          {
            topk_precision_ = double_value;
          }
        }
      }
      return ret;
    }


    int64_t ObScanParam::get_limit_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // LIMIT_PARAM_FIELD
      obj.set_ext(ObActionFlag::LIMIT_PARAM_FIELD);
      total_size += obj.get_serialize_size();
      // offset
      obj.set_int(limit_offset_);
      total_size += obj.get_serialize_size();
      // count
      obj.set_int(limit_count_);
      total_size += obj.get_serialize_size();

      return total_size;
    }

    int64_t ObScanParam::get_topk_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // TOPK_PARAM_FIELD
      obj.set_ext(ObActionFlag::TOPK_PARAM_FIELD);
      total_size += obj.get_serialize_size();

      /// topk
      obj.set_int(sharding_minimum_row_count_);
      total_size += obj.get_serialize_size();

      /// precision
      obj.set_double(topk_precision_);
      total_size += obj.get_serialize_size();
      return total_size;
    }

    int ObScanParam::serialize_groupby_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_ext(ObActionFlag::GROUPBY_PARAM_FIELD);
      ret = obj.serialize(buf,buf_len,pos);
      if (OB_SUCCESS == ret && group_by_param_.get_aggregate_row_width() > 0)
      {
        ret = group_by_param_.serialize(buf,buf_len,pos);
      }
      return ret;
    }

    int ObScanParam::deserialize_groupby_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      return group_by_param_.deserialize(buf, data_len, pos);
    }

    int64_t ObScanParam::get_groupby_param_serialize_size(void) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::GROUPBY_PARAM_FIELD);
      int64_t total_size = obj.get_serialize_size();
      if (group_by_param_.get_aggregate_row_width() > 0)
      {
        total_size += group_by_param_.get_serialize_size();
      }
      return total_size;
    }

    int ObScanParam::serialize_filter_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::SELECT_CLAUSE_WHERE_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = condition_filter_.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    int ObScanParam::deserialize_filter_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      return condition_filter_.deserialize(buf, data_len, pos);
    }

    int64_t ObScanParam::get_filter_param_serialize_size(void) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::SELECT_CLAUSE_WHERE_FIELD);
      int64_t total_size = obj.get_serialize_size();
      total_size += condition_filter_.get_serialize_size();
      return total_size;
    }

    // SELECT_CLAUSE_COMP_COLUMN_FIELD
    int ObScanParam::serialize_composite_column_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      int err = OB_SUCCESS;
      ObObj obj;
      obj.set_ext(ObActionFlag::SELECT_CLAUSE_COMP_COLUMN_FIELD);
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.serialize(buf,buf_len,pos))))
      {
        TBSYS_LOG(WARN,"fail to serialize SELECT_CLAUSE_COMP_COLUMN_FIELD ext obj");
      }
      for (int64_t idx = 0; OB_SUCCESS == err && idx < select_comp_column_list_.get_array_index(); idx ++)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = select_comp_columns_[idx].serialize(buf,buf_len,pos))))
        {
          TBSYS_LOG(WARN,"fail to serialize composite column [idx:%ld, err:%d]", idx, err);
        }
      }

      return err;
    }


    int ObScanParam::malloc_composite_columns()
    {
      int err = OB_SUCCESS;
      select_comp_columns_ = reinterpret_cast<ObCompositeColumn*>(ob_malloc(sizeof(ObCompositeColumn)*OB_MAX_COLUMN_NUMBER, ObModIds::OB_SCAN_PARAM));
      if (NULL == select_comp_columns_)
      {
        TBSYS_LOG(WARN,"fail to allocate memory");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new(select_comp_columns_) ObCompositeColumn[OB_MAX_COLUMN_GROUP_NUMBER];
        select_comp_column_list_.init(OB_MAX_COLUMN_NUMBER, select_comp_columns_);
      }
      return err;
    }

    int ObScanParam::deserialize_composite_column_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int err = OB_SUCCESS;

      ObCompositeColumn comp_column;
      while (OB_SUCCESS == err)
      {
        if ((OB_SUCCESS != (err = comp_column.deserialize(buf, data_len, pos)))
          && (OB_UNKNOWN_OBJ != err))
        {
          TBSYS_LOG(WARN,"fail to decode composite column [err:%d]", err);
        }
        if ((NULL == select_comp_columns_) && (OB_SUCCESS == err))
        {
          err = malloc_composite_columns();
        }
        if ((OB_SUCCESS == err) && (!select_comp_column_list_.push_back(comp_column)))
        {
          TBSYS_LOG(WARN,"composite column list is full");
          err = OB_ARRAY_OUT_OF_RANGE;
        }
      }
      if (OB_UNKNOWN_OBJ == err)
      {
        err = OB_SUCCESS;
      }
      return err;
    }

    int64_t ObScanParam::get_composite_column_param_serialize_size(void) const
    {
      int64_t total_len = 0;
      ObObj obj;
      obj.set_ext(ObActionFlag::SELECT_CLAUSE_COMP_COLUMN_FIELD);
      total_len += obj.get_serialize_size();
      for (int64_t idx = 0; idx < select_comp_column_list_.get_array_index(); idx ++)
      {
        total_len += select_comp_columns_[idx].get_serialize_size();
      }
      return total_len;
    }

    // SELECT_CLAUSE_RETURN_INFO_FIELD
    int ObScanParam::serialize_return_info_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      int err = OB_SUCCESS;
      if ((get_return_info_size() > 0 )
        && (get_return_info_size() != basic_column_id_list_.get_array_index()
        + basic_column_list_.get_array_index() + select_comp_column_list_.get_array_index()))
      {
        TBSYS_LOG(WARN,"count of return info not coincident with count of columns [count_of_return_info:%ld,"
          "count_of_columns:%ld]",get_return_info_size() ,
          basic_column_id_list_.get_array_index()
          + basic_column_list_.get_array_index() + select_comp_column_list_.get_array_index());
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        ObObj obj;
        obj.set_ext(ObActionFlag::SELECT_CLAUSE_RETURN_INFO_FIELD);
        if (OB_SUCCESS != (err = obj.serialize(buf,buf_len, pos)))
        {
          TBSYS_LOG(WARN,"fail to serialize SELECT_CLAUSE_RETURN_INFO_FIELD [err:%d]", err);
        }
        ObObj bool_obj;
        for (int64_t idx = 0; OB_SUCCESS == err && idx < get_return_info_size(); idx ++)
        {
          bool_obj.set_int(*is_return(idx));
          if (OB_SUCCESS != (err = bool_obj.serialize(buf, buf_len, pos)))
          {
            TBSYS_LOG(WARN,"fail to serialize select return info [idx:%ld,err:%d]", idx, err);
          }
        }
      }
      return err;
    }

    int ObScanParam::deserialize_return_info_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int err = OB_SUCCESS;
      basic_return_info_list_.clear();
      comp_return_info_list_.clear();
      ObObj obj;
      int64_t old_pos = pos;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,data_len,pos))))
      {
        TBSYS_LOG(WARN,"fail to deserialize first obj [err:%d]", err);
      }
      int64_t basic_column_count = std::max<int64_t>(basic_column_list_.get_array_index(),
        basic_column_id_list_.get_array_index());
      for (int64_t i = 0;
        (i < basic_column_count) && (obj.get_type() == ObIntType) && (OB_SUCCESS == err);
        i++)
      {
        int64_t val = 0;
        old_pos = pos;
        if (OB_SUCCESS != (err = obj.get_int(val)))
        {
          TBSYS_LOG(WARN,"fail to get bool val from obj [err:%d,type:%d]", err, obj.get_type());
        }
        if ((OB_SUCCESS == err) && (!basic_return_info_list_.push_back(val)))
        {
          TBSYS_LOG(WARN,"select return info list is full");
          err = OB_ARRAY_OUT_OF_RANGE;
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,data_len, pos))))
        {
          TBSYS_LOG(WARN,"fail to deserialize obj [err:%d]", err);
        }
      }
      int64_t comp_column_count = select_comp_column_list_.get_array_index();
      for (int64_t i = 0;
        (i < comp_column_count) && (obj.get_type() == ObIntType) && (OB_SUCCESS == err);
        i++)
      {
        int64_t val = 0;
        old_pos = pos;
        if (OB_SUCCESS != (err = obj.get_int(val)))
        {
          TBSYS_LOG(WARN,"fail to get bool val from obj [err:%d,type:%d]", err, obj.get_type());
        }
        if ((OB_SUCCESS == err) && (!comp_return_info_list_.push_back(val)))
        {
          TBSYS_LOG(WARN,"select return info list is full");
          err = OB_ARRAY_OUT_OF_RANGE;
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,data_len, pos))))
        {
          TBSYS_LOG(WARN,"fail to deserialize obj [err:%d]", err);
        }
      }
      pos = old_pos;
      return err;
    }

    int64_t ObScanParam::get_return_info_serialize_size(void) const
    {
      int64_t total_len = 0;
      ObObj obj;
      obj.set_ext(ObActionFlag::SELECT_CLAUSE_RETURN_INFO_FIELD);
      total_len += obj.get_serialize_size();
      ObObj bool_obj;
      for (int64_t idx = 0; idx < get_return_info_size(); idx ++)
      {
        bool_obj.set_int(*is_return(idx));
        total_len += bool_obj.get_serialize_size();
      }
      return total_len;
    }

    int ObScanParam::serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      return obj.serialize(buf, buf_len, pos);
    }

    int ObScanParam::deserialize_end_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      // no data
      UNUSED(buf);
      UNUSED(data_len);
      UNUSED(pos);
      return 0;
    }

    int64_t ObScanParam::get_end_param_serialize_size(void) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      return obj.get_serialize_size();
    }

    DEFINE_SERIALIZE(ObScanParam)
    {
      int ret = OB_SUCCESS;
      if (orderby_column_name_list_.get_array_index() > 0
        && orderby_column_id_list_.get_array_index() > 0)
      {
        TBSYS_LOG(WARN,"use column id or column name, but not both");
        ret = OB_INVALID_ARGUMENT;
      }

      if (basic_column_list_.get_array_index() > 0
        && basic_column_id_list_.get_array_index() > 0)
      {
        TBSYS_LOG(WARN,"use column id or column name, but not both");
        ret = OB_INVALID_ARGUMENT;
      }

      if ((orderby_column_name_list_.get_array_index() > 0
        &&orderby_column_name_list_.get_array_index() != orderby_order_list_.get_array_index())
        || (orderby_column_id_list_.get_array_index() > 0
        &&orderby_column_id_list_.get_array_index() != orderby_order_list_.get_array_index())
        )
      {
        TBSYS_LOG(WARN,"sizeof order list not coincident with column id/name list");
        ret = OB_ERR_UNEXPECTED;
      }

      // RESERVE_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = ObReadParam::serialize_reserve_param(buf, buf_len, pos);
      }


      // BASIC_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_basic_param(buf, buf_len, pos);
      }

      // COLUMN_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_column_param(buf, buf_len, pos);
      }

      // SELECT_CLAUSE_COMP_COLUMN_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_composite_column_param(buf, buf_len, pos);
      }

      // SELECT_CLAUSE_WHERE_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_filter_param(buf, buf_len, pos);
      }

      // SELECT_CLAUSE_RETURN_INFO_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_return_info_param(buf, buf_len, pos);
      }

      // GROUPBY_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_groupby_param(buf,buf_len,pos);
      }

      // SORT_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_sort_param(buf, buf_len, pos);
      }

      // LIMIT_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_limit_param(buf, buf_len, pos);
      }
      // topk
      if (OB_SUCCESS == ret)
      {
        ret = serialize_topk_param(buf,buf_len,pos);
      }

      // END_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        ret = serialize_end_param(buf, buf_len, pos);
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObScanParam)
    {
      // reset contents
      reset();
      ObObj obj;
      int ret = OB_SUCCESS;
      while (OB_SUCCESS == ret)
      {
        do
        {
          ret = obj.deserialize(buf, data_len, pos);
        } while (OB_SUCCESS == ret && ObExtendType != obj.get_type());

        if (OB_SUCCESS == ret && ObActionFlag::END_PARAM_FIELD != obj.get_ext())
        {
          switch (obj.get_ext())
          {
          case ObActionFlag::RESERVE_PARAM_FIELD:
            {
              ret = ObReadParam::deserialize_reserve_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::BASIC_PARAM_FIELD:
            {
              ret = deserialize_basic_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::COLUMN_PARAM_FIELD:
            {
              ret = deserialize_column_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::SORT_PARAM_FIELD:
            {
              ret = deserialize_sort_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::LIMIT_PARAM_FIELD:
            {
              ret = deserialize_limit_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::SELECT_CLAUSE_WHERE_FIELD:
            {
              ret = deserialize_filter_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::GROUPBY_PARAM_FIELD:
            {
              ret = deserialize_groupby_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::SELECT_CLAUSE_COMP_COLUMN_FIELD:
            {
              ret = deserialize_composite_column_param(buf,data_len,pos);
              break;
            }
          case ObActionFlag::SELECT_CLAUSE_RETURN_INFO_FIELD:
            {
              ret = deserialize_return_info_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::TOPK_PARAM_FIELD:
            {
              ret = deserialize_topk_param(buf,data_len,pos);
              break;
            }
          default:
            {
              // deserialize next cell
              // ret = obj.deserialize(buf, data_len, pos);
              break;
            }
          }
        }
        else
        {
          break;
        }
      }
      /// Compatible with old client
      if ((OB_SUCCESS == ret) && (0 == get_return_info_size()))
      {
        int64_t basic_column_count = basic_column_list_.get_array_index() + basic_column_id_list_.get_array_index() ;
        for (int64_t i = 0; (OB_SUCCESS == ret) && i < basic_column_count; i++)
        {
          if (!basic_return_info_list_.push_back(true))
          {
            TBSYS_LOG(WARN,"return info list is full");
            ret = OB_ARRAY_OUT_OF_RANGE;
          }
        }
        int64_t comp_column_count =  select_comp_column_list_.get_array_index();
        for (int64_t i = 0; (OB_SUCCESS == ret) && i < comp_column_count; i++)
        {
          if (!comp_return_info_list_.push_back(true))
          {
            TBSYS_LOG(WARN,"return info list is full");
            ret = OB_ARRAY_OUT_OF_RANGE;
          }
        }
      }
      if ((OB_SUCCESS == ret) && (get_return_info_size() != basic_column_id_list_.get_array_index()
        + basic_column_list_.get_array_index() + select_comp_column_list_.get_array_index()))
      {
        TBSYS_LOG(WARN,"count of return info not coincident with count of columns [count_of_return_info:%ld,"
          "count_of_columns:%ld]",get_return_info_size() ,
          basic_column_id_list_.get_array_index()
          + basic_column_list_.get_array_index() + select_comp_column_list_.get_array_index());
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObScanParam)
    {
      int64_t total_size = get_basic_param_serialize_size();
      total_size += ObReadParam::get_reserve_param_serialize_size();
      total_size += get_column_param_serialize_size();
      total_size += get_composite_column_param_serialize_size();
      total_size += get_filter_param_serialize_size();
      total_size += get_return_info_serialize_size();
      total_size += get_groupby_param_serialize_size();
      total_size += get_sort_param_serialize_size();
      total_size += get_limit_param_serialize_size();
      total_size += get_topk_param_serialize_size();
      total_size += get_end_param_serialize_size();
      return total_size;
    }


    void ObScanParam::dump(void) const
    {
      int ret = OB_SUCCESS;
      if (orderby_column_name_list_.get_array_index() > 0
        && orderby_column_id_list_.get_array_index() > 0)
      {
        TBSYS_LOG(WARN,"use column id or column name, but not both");
        ret = OB_INVALID_ARGUMENT;
      }

      if (basic_column_list_.get_array_index() > 0
        && basic_column_id_list_.get_array_index() > 0)
      {
        TBSYS_LOG(WARN,"use column id or column name, but not both");
        ret = OB_INVALID_ARGUMENT;
      }

      if ((orderby_column_name_list_.get_array_index() > 0
        &&orderby_column_name_list_.get_array_index() != orderby_order_list_.get_array_index())
        || (orderby_column_id_list_.get_array_index() > 0
        &&orderby_column_id_list_.get_array_index() != orderby_order_list_.get_array_index())
        )
      {
        TBSYS_LOG(WARN,"sizeof order list not coincident with column id/name list");
        ret = OB_ERR_UNEXPECTED;
      }

      // BASIC_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        dump_basic_param();
      }

      // COLUMN_PARAM_FIELD
      if (ret == OB_SUCCESS)
      {
        dump_column_param();
      }

      // SELECT_CLAUSE_COMP_COLUMN_FIELD
      if (OB_SUCCESS == ret)
      {
        dump_composite_column_param();
      }

      // SELECT_CLAUSE_WHERE_FIELD
      if (OB_SUCCESS == ret)
      {
        dump_filter_param();
      }

      // SELECT_CLAUSE_RETURN_INFO_FIELD
      if (OB_SUCCESS == ret)
      {
        dump_return_info_param();
      }

      // GROUPBY_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        dump_groupby_param();
      }

      // SORT_PARAM_FIELD
      if (ret == OB_SUCCESS)
      {
        dump_sort_param();
      }

      // LIMIT_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        dump_limit_param();
      }
      // topk
      if (OB_SUCCESS == ret)
      {
        dump_topk_param();
      }
    }

    void ObScanParam::dump_basic_param() const
    {
      TBSYS_LOG(INFO, "[dump] [table_name_=%.*s][table_id_=%lu]", table_name_.length(), table_name_.ptr(), table_id_);
      TBSYS_LOG(INFO, "[dump] scan range info: %s", to_cstring(range_));
      TBSYS_LOG(INFO, "[dump] [scan_direction=%s]", scan_flag_.direction_ == 0? "FORWARD" : "BACKWARD");
    }


    void ObScanParam::dump_column_param() const
    {
      int ret = OB_SUCCESS;
      int64_t column_size = basic_column_list_.get_array_index();
      int64_t column_id_size = basic_column_id_list_.get_array_index();
      if ((column_size > 0) && (column_id_size > 0))
      {
        TBSYS_LOG(ERROR, "[dump] check column size failed");
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        for (int64_t i = 0; i < column_size; ++i)
        {
          TBSYS_LOG(INFO, "[dump] basic_column_names_[%ld]=%.*s",
              i, basic_column_names_[i].length(), basic_column_names_[i].ptr());
        }

        for (int64_t i = 0; i < column_id_size; ++i)
        {
          TBSYS_LOG(INFO, "[dump] basic_column_ids_[%ld]=%lu",
              i, basic_column_ids_[i]);
        }
      }
    }


    void ObScanParam::dump_composite_column_param() const
    {
      TBSYS_LOG(INFO, "[dump] composite column size=%ld", select_comp_column_list_.get_array_index());
    }


    void ObScanParam::dump_filter_param() const
    {
      TBSYS_LOG(INFO, "[dump] condition filter count=%ld", condition_filter_.get_count());
    }


    void ObScanParam::dump_return_info_param() const
    {
      TBSYS_LOG(INFO, "[dump] return info size=%ld", get_return_info_size());
    }


    void ObScanParam::dump_groupby_param() const
    {
      TBSYS_LOG(INFO, "[dump] groupby info\n "
          "\tget_groupby_columns=%ld\n"
          "\tget_return_columns=%ld\n"
          "\tget_aggregate_columns=%ld\n"
          "\tget_composite_columns=%ld\n"
          "\tget_return_infos=%ld\n"
          "\tget_having_condition=%ld\n" ,
          group_by_param_.get_groupby_columns().get_array_index(),
          group_by_param_.get_return_columns().get_array_index(),
          group_by_param_.get_aggregate_columns().get_array_index(),
          group_by_param_.get_composite_columns().get_array_index(),
          group_by_param_.get_return_infos().get_array_index(),
          group_by_param_.get_having_condition().get_count()
          );
    }


    void ObScanParam::dump_sort_param() const
    {

    }


    void ObScanParam::dump_limit_param() const
    {
      TBSYS_LOG(INFO, "[dump] [limit_offset_=%ld][limit_count=%ld]", limit_offset_, limit_count_);
    }


    void ObScanParam::dump_topk_param() const
    {
    }

  } /* common */
} /* oceanbase */
