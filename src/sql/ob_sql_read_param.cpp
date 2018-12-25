#include "common/ob_object.h"
#include "common/ob_action_flag.h"
#include "common/ob_schema.h"
#include "common/ob_rowkey_helper.h"
#include "ob_sql_read_param.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql
  {
    ObSqlReadParam::ObSqlReadParam() :
      is_read_master_(0), is_result_cached_(0), data_version_(OB_NEWEST_DATA_VERSION),
      table_id_(OB_INVALID_ID), renamed_table_id_(OB_INVALID_ID), only_static_data_(false),
      project_(), scalar_agg_(NULL), group_(NULL), group_columns_sort_(), limit_(), filter_(),
      has_project_(false), has_scalar_agg_(false), has_group_(false), has_group_columns_sort_(false),
      is_indexed_group_(false), has_limit_(false), has_filter_(false),//add liumz, [optimize group_order by index]20170419
      is_listagg_(false),//add gaojt [ListAgg][JHOBv0.1]20150104
      sort_for_listagg_()//add gaojt [ListAgg][JHOBv0.1]20150104
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      ,data_mark_param_()
      //add duyr 20160531:e
    //del by maosy
    {
      reset();
    }

    void ObSqlReadParam::reset()
    {
      is_read_master_ = 0;
      is_result_cached_ = 0;
      data_version_ = OB_NEWEST_DATA_VERSION;
      table_id_ = OB_INVALID_ID;
      renamed_table_id_ = OB_INVALID_ID;
      if (has_scalar_agg_)
      {
        if (NULL != scalar_agg_)
        {
          scalar_agg_->reset();
        }
        has_scalar_agg_ = false;
      }
      if (has_group_)
      {
        if (NULL != group_)
        {
          group_->reset();
        }
        has_group_ = false;
      }
      if (has_limit_)
      {
        limit_.reset();
        has_limit_ = false;
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      if(is_listagg_)
      {
          sort_for_listagg_.reset();
          is_listagg_ = false;
      }
      /* add 20150104:e */
      if (has_group_columns_sort_)
      {
        group_columns_sort_.reset();
        has_group_columns_sort_ = false;
        is_indexed_group_ = false;//add liumz, [optimize group_order by index]20170419
      }
      filter_.reset();
      project_.reset();
      has_project_ = false;
      has_filter_ = false;
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      //data_mark_param_.reset();
      //add duyr 20160531:e
      //del e
    }

    ObSqlReadParam::~ObSqlReadParam()
    {
      if (NULL != scalar_agg_)
      {
        scalar_agg_->~ObScalarAggregate();
        ob_free(scalar_agg_);
        scalar_agg_ = NULL;
      }
      if (NULL != group_)
      {
        group_->~ObMergeGroupBy();
        ob_free(group_);
        group_ = NULL;
      }
    }

    int ObSqlReadParam::set_project(const ObProject &project)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        project_.assign(&project);
        has_project_ = true;
      }
      return ret;
    }
    int ObSqlReadParam::add_output_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      ret = project_.add_output_column(expr);
      if (OB_SUCCESS == ret)
      {
        has_project_ = true;
      }
      return ret;
    }

    const ObProject & ObSqlReadParam::get_project() const
    {
      return project_;
    }

    int ObSqlReadParam::set_group_columns_sort(const ObSort &sort)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        group_columns_sort_.assign(&sort);
        has_group_columns_sort_ = true;
      }
      return ret;
    }

    int ObSqlReadParam::set_filter(const ObFilter &filter)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        filter_.assign(&filter);
        if(filter_.count ()==0)
             has_filter_ = false;
        else
         has_filter_ = true;
      }
      return ret;
    }

    int ObSqlReadParam::add_filter(ObSqlExpression *cond)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = filter_.add_filter(cond)))
      {
        TBSYS_LOG(WARN, "fail to add filter. ret=%d", ret);
      }
      else
      {
        has_filter_ = true;
      }
      return ret;
    }

    int ObSqlReadParam::add_group_column(const uint64_t tid, const uint64_t cid)
    {
      int ret = OB_SUCCESS;
      if (has_scalar_agg_)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Can not adding group column after adding aggregate function(s). ret=%d", ret);
      }
      else
      {
        if (NULL == group_)
        {
          group_ = OB_NEW(ObMergeGroupBy, ObModIds::OB_SQL_MERGE_GROUPBY);
          if (group_)
          {
            group_->set_phy_plan(project_.get_phy_plan());
          }
        }
        if (NULL == group_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "failed to alloc memory");
        }
        else if (!is_indexed_group_ && (ret = group_columns_sort_.add_sort_column(tid, cid, true)) != OB_SUCCESS)//mod liumz, [optimize group_order by index]20170419
        {
          TBSYS_LOG(WARN, "Add sort column of ObSqlReadParam sort operator failed. ret=%d", ret);
        }
        else if ((ret = group_->add_group_column(tid, cid)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add group column of ObSqlReadParam group operator failed. ret=%d", ret);
        }
        else if (!has_group_)
        {
          has_group_columns_sort_ = true;
          has_group_ = true;
        }
      }

      return ret;
    }
    //add gaojt [ListAgg][JHOBv0.1]20150104:b
    int ObSqlReadParam::add_sort_column_for_listagg(const uint64_t tid, const uint64_t cid,bool is_asc)
    {
        int ret = OB_SUCCESS;
        if ((ret = sort_for_listagg_.add_sort_column(tid, cid, is_asc)) != OB_SUCCESS)
        {
           TBSYS_LOG(ERROR, "Add sort column of ObSqlReadParam sort operator failed. ret=%d", ret);
        }
        if(OB_SUCCESS == ret)
        {
            is_listagg_ = true;
        }
        return ret;
    }
    /* add 20150104:e */
    int ObSqlReadParam::add_aggr_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      if (has_group_)
      {
        if ((ret = group_->add_aggr_column(expr)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add aggregate function of ObSqlReadParam group operator failed. ret=%d", ret);
        }
      }
      else
      {
        has_scalar_agg_ = true;
        if (NULL == scalar_agg_)
        {
          scalar_agg_ = OB_NEW(ObScalarAggregate, ObModIds::OB_SQL_SCALAR_AGGR);
          if (scalar_agg_)
          {
            scalar_agg_->set_phy_plan(project_.get_phy_plan());
          }
        }
        if (NULL == scalar_agg_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "failed to alloc memory");
        }
        else if ((ret = scalar_agg_->add_aggr_column(expr)) != OB_SUCCESS)
        {

          TBSYS_LOG(WARN, "Add aggregate function of ObSqlReadParam scalar aggregate operator failed. ret=%d", ret);
        }
      }
      return ret;
    }

    const ObFilter & ObSqlReadParam::get_filter() const
    {
      return filter_;
    }
    //add fanqiushi_index
    int ObSqlReadParam::reset_project_and_filter()
    {
        project_.reset();
        filter_.reset();
        return OB_SUCCESS;
    }
    //add wanglei [semi join] 20160111
    int ObSqlReadParam::reuse_project_and_filter()
    {
        project_.reuse();
        filter_.reuse();
        return OB_SUCCESS;
    }
    //add:e
    //add wanglei [semi join update] 20160415:b
    ObFilter & ObSqlReadParam::get_raw_filter()
    {
      return filter_;
    }
    //add wanglei [semi join update] 20160415:e
    int ObSqlReadParam::set_limit(const ObLimit &limit)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        limit_.assign(&limit);
        has_limit_ = true;
      }
      return ret;
    }

    int ObSqlReadParam::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = limit_.set_limit(limit, offset)))
      {
        TBSYS_LOG(WARN, "fail to add filter. ret=%d", ret);
      }
      else
      {
        has_limit_ = true;
      }
      return ret;
    }

    const ObLimit & ObSqlReadParam::get_limit() const
    {
      return limit_;
    }

    void ObSqlReadParam::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      project_.set_phy_plan(the_plan);
      if (scalar_agg_)
      {
        scalar_agg_->set_phy_plan(the_plan);
      }
      if (group_)
      {
        group_->set_phy_plan(the_plan);
      }
      group_columns_sort_.set_phy_plan(the_plan);
      limit_.set_phy_plan(the_plan);
      filter_.set_phy_plan(the_plan);
      sort_for_listagg_.set_phy_plan(the_plan);// add by gaojt
    }

    ////////////////////// SERIALIZATION ///////////////////////
    int ObSqlReadParam::serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      // read consistency
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_is_read_consistency());
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      // result cached
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_is_result_cached());
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      // data version
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_data_version());
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      // table id
      if (OB_SUCCESS == ret)
      {
        if (OB_INVALID_ID != table_id_)
        {
          obj.set_int(table_id_);
          if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to serialize table id = %lu", table_id_);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "Invalid table_id_. table_id_=%ld", table_id_);
          ret = OB_INVALID_ARGUMENT;
        }
      }
      // renamed table id
      if (OB_SUCCESS == ret)
      {
        if (OB_INVALID_ID != renamed_table_id_)
        {
          obj.set_int(renamed_table_id_);
          if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to serialize renamed table id = %lu", renamed_table_id_);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "Invalid renamed_table_id_. renamed_table_id_=%ld", renamed_table_id_);
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == ret)
      {
        obj.set_bool(only_static_data_);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize bool:ret[%d]", ret);
        }
      }
      return ret;
    }


    int ObSqlReadParam::deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      // read consistency
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            //is read consistency
            set_is_read_consistency(int_value);
          }
          else
          {
            TBSYS_LOG(WARN, "fail to get int. obj type=%d", obj.get_type());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d", ret);
        }
      }
      // result cached
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            //is read consistency
            set_is_result_cached(int_value > 0);
          }
          else
          {
            TBSYS_LOG(WARN, "fail to get int. obj type=%d", obj.get_type());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d", ret);
        }
      }
      // data_version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            // data version
            set_data_version(int_value);
          }
          else
          {
            TBSYS_LOG(WARN, "fail to get int. obj type=%d", obj.get_type());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d", ret);
        }
      }
      // table id
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            table_id_ = int_value;
          }
          else
          {
            TBSYS_LOG(WARN, "fail to get int. obj type=%d", obj.get_type());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d", ret);
        }
      }
      // renamed table id
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            renamed_table_id_ = int_value;
          }
          else
          {
            TBSYS_LOG(WARN, "fail to get int. obj type=%d", obj.get_type());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to deserialize obj:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = obj.get_bool(only_static_data_)))
        {
          TBSYS_LOG(WARN, "fail to get bool:ret[%d]", ret);
        }
      }
      return ret;
    }

    int ObSqlReadParam::serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      return obj.serialize(buf, buf_len, pos);
    }

    
    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140409:b

    /*Exp:remove expression which has sub_query but not use bloomfilter 
    * make stmt pass the first check 
	* if after remove filter_ has no expr,must set attribute has_filter_ as false 
    */
    int ObSqlReadParam::remove_or_sub_query_expr()
    {
      int ret = OB_SUCCESS;
      filter_.remove_or_sub_query_expr();
      if(0 == filter_.count())
		has_filter_ = false;
      return ret;
    }

    /*Exp:update sub_query num
    * if direct bind sub_query result to main query,
    * do not treat it as a sub_query any more
    */
    void ObSqlReadParam::update_sub_query_num()
    {
      filter_.update_sub_query_num();
    }
	//add 20140409:e
    DEFINE_SERIALIZE(ObSqlReadParam)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      // BASIC_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if(OB_SUCCESS != (ret = serialize_basic_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // SQL_PROJECT_PARAM_FIELD
      if (OB_SUCCESS == ret && has_project_)
      {
        obj.set_ext(ObActionFlag::SQL_PROJECT_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = project_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize project param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // SCALAR_AGG_PARAM_FIELD
      if (OB_SUCCESS == ret && has_scalar_agg_)
      {
        obj.set_ext(ObActionFlag::SQL_SCALAR_AGG_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = scalar_agg_->serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize scalar aggregation param. buf=%p, buf_len=%ld, pos=%ld, ret=%d",
              buf, buf_len, pos, ret);
        }
      }

      // GROUP_SORT_PARAM_FIELD
      if (OB_SUCCESS == ret && has_group_columns_sort_)
      {
        obj.set_ext(ObActionFlag::SQL_GROUP_SORT_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = group_columns_sort_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize group column sort param. buf=%p, buf_len=%ld, pos=%ld, ret=%d",
              buf, buf_len, pos, ret);
        }
      }
      // SORT_FOR_LISTAGG_FIELD
      if (OB_SUCCESS == ret && is_listagg_)
      {
        obj.set_ext(ObActionFlag::SORT_FOR_LISTAGG_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = sort_for_listagg_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize listagg sort param. buf=%p, buf_len=%ld, pos=%ld, ret=%d",
              buf, buf_len, pos, ret);
        }
      }
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      if (OB_SUCCESS == ret && data_mark_param_.is_valid())
//      {
//          obj.set_ext(ObActionFlag::DATA_MARK_PARAM_FIELD);
//          if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
//          {
//            TBSYS_LOG(WARN, "fail to serialize data mark param field obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
//          }
//          else if (OB_SUCCESS != (ret = data_mark_param_.serialize(buf, buf_len, pos)))
//          {
//            TBSYS_LOG(WARN, "fail to serialize data_mark param. buf=%p, buf_len=%ld, pos=%ld, ret=%d",
//                buf, buf_len, pos, ret);
//          }
//      }
      //add duyr 20160531:e
//del by maosy
      // GROUP_BY_PARAM_FIELD
      if (OB_SUCCESS == ret && has_group_)
      {
        obj.set_ext(ObActionFlag::SQL_GROUP_BY_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = group_->serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize group param. buf=%p, buf_len=%ld, pos=%ld, ret=%d",
              buf, buf_len, pos, ret);
        }
      }

      // LIMIT_PARAM_FIELD
      if (OB_SUCCESS == ret && has_limit_)
      {
        obj.set_ext(ObActionFlag::SQL_LIMIT_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = limit_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize limit param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // FILTER_PARAM_FIELD
      if (OB_SUCCESS == ret && has_filter_)
      {
        obj.set_ext(ObActionFlag::SQL_FILTER_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = filter_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize filter param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // END_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialize_end_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize end param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSqlReadParam)
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
            case ObActionFlag::BASIC_PARAM_FIELD:
              {
                if (OB_SUCCESS != (ret = deserialize_basic_param(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to dersialize basic param. ret=%d", ret);
                }
                break;
              }
            case ObActionFlag::SQL_PROJECT_PARAM_FIELD:
              {
                if (OB_SUCCESS != (ret = project_.deserialize(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize project. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                      buf, data_len, pos, ret);
                }
                else
                {
                  has_project_ = true;
                }
                break;
              }
            case ObActionFlag::SQL_SCALAR_AGG_PARAM_FIELD:
              {
                if (NULL == scalar_agg_)
                {
                  scalar_agg_ = OB_NEW(ObScalarAggregate, ObModIds::OB_SQL_SCALAR_AGGR);
                }
                if (NULL == scalar_agg_)
                {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  TBSYS_LOG(WARN, "no memory");
                }
                else if (OB_SUCCESS != (ret = scalar_agg_->deserialize(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize scalar aggregation. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                      buf, data_len, pos, ret);
                }
                else
                {
                  has_scalar_agg_ = true;
                }
                break;
              }
            case ObActionFlag::SQL_GROUP_SORT_PARAM_FIELD:
              {
                if (OB_SUCCESS != (ret = group_columns_sort_.deserialize(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize group column sort. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                      buf, data_len, pos, ret);
                }
                else
                {
                  has_group_columns_sort_ = true;
                }
                break;
              }
          //add gaojt [ListAgg][JHOBv0.1]20150104:b
          case ObActionFlag::SORT_FOR_LISTAGG_FIELD:
            {
              if (OB_SUCCESS != (ret = sort_for_listagg_.deserialize(buf, data_len, pos)))
              {
                TBSYS_LOG(WARN, "fail to deserialize group column sort. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                    buf, data_len, pos, ret);
              }
              else
              {
                is_listagg_ = true;
              }
              break;
            }
          /* add 20150104:e*/
                  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
          //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//            case ObActionFlag::DATA_MARK_PARAM_FIELD:
//            {
//              if (OB_SUCCESS != (ret = data_mark_param_.deserialize(buf, data_len, pos)))
//              {
//                TBSYS_LOG(WARN, "fail to deserialize data mark param. buf=%p, data_len=%ld, pos=%ld, ret=%d",
//                    buf, data_len, pos, ret);
//              }
//              break;
//            }
          //add duyr 20160531:e
      //del by maosy
            case ObActionFlag::SQL_GROUP_BY_PARAM_FIELD:
              {
                if (NULL == group_)
                {
                  group_ = OB_NEW(ObMergeGroupBy, ObModIds::OB_SQL_MERGE_GROUPBY);
                }

                if (NULL == group_)
                {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  TBSYS_LOG(WARN, "no memory");
                }
                else if (OB_SUCCESS != (ret = group_->deserialize(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize group by. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                      buf, data_len, pos, ret);
                }
                else
                {
                  has_group_ = true;
                }
                break;
              }
            case ObActionFlag::SQL_LIMIT_PARAM_FIELD:
              {
                if (OB_SUCCESS != (ret = limit_.deserialize(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize limit. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                      buf, data_len, pos, ret);
                }
                else
                {
                  has_limit_ = true;
                }
                break;
              }
            case ObActionFlag::SQL_FILTER_PARAM_FIELD:
              {
                if (OB_SUCCESS != (ret = filter_.deserialize(buf, data_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to deserialize filter. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                      buf, data_len, pos, ret);
                }
                else
                {
                  has_filter_ = true;
                }
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
      return ret;
    }


    int64_t ObSqlReadParam::get_basic_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // consistency
      obj.set_int(get_is_read_consistency());
      total_size += obj.get_serialize_size();
      // result cached
      obj.set_int(get_is_result_cached());
      total_size += obj.get_serialize_size();
      // data version
      obj.set_int(get_data_version());
      total_size += obj.get_serialize_size();
      // table id
      obj.set_int(table_id_);
      total_size += obj.get_serialize_size();
      // renamed table id
      obj.set_int(renamed_table_id_);
      total_size += obj.get_serialize_size();
      // only_static_data
      obj.set_bool(only_static_data_);
      total_size += obj.get_serialize_size();
      return total_size;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSqlReadParam)
    {
      ObObj obj;
      int64_t total_size = 0;
      // BASIC_PARAM_FIELD
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      total_size += obj.get_serialize_size();
      total_size += get_basic_param_serialize_size();

      // Project Field
      if (has_project_)
      {
        obj.set_ext(ObActionFlag::SQL_PROJECT_PARAM_FIELD);
        total_size += obj.get_serialize_size();
        total_size += project_.get_serialize_size();
      }
      if (has_scalar_agg_)
      {
        obj.set_ext(ObActionFlag::SQL_SCALAR_AGG_PARAM_FIELD);
        total_size += obj.get_serialize_size();
        total_size += scalar_agg_->get_serialize_size();
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      if (is_listagg_)
      {
        obj.set_ext(ObActionFlag::SORT_FOR_LISTAGG_FIELD);
        total_size += obj.get_serialize_size();
        total_size += scalar_agg_->get_serialize_size();
      }
      /* add 20150104:e */
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      if (data_mark_param_.is_valid())
//      {
//        obj.set_ext(ObActionFlag::DATA_MARK_PARAM_FIELD);
//        total_size += obj.get_serialize_size();
//        total_size += data_mark_param_.get_serialize_size();
//      }
      //add duyr 20160531:e
    //del by maosy
      if (has_group_columns_sort_)
      {
        obj.set_ext(ObActionFlag::SQL_GROUP_SORT_PARAM_FIELD);
        total_size += obj.get_serialize_size();
        total_size += group_columns_sort_.get_serialize_size();
      }
      if (has_group_)
      {
        obj.set_ext(ObActionFlag::SQL_GROUP_BY_PARAM_FIELD);
        total_size += obj.get_serialize_size();
        total_size += group_->get_serialize_size();
      }
      if (has_limit_)
      {
        obj.set_ext(ObActionFlag::SQL_LIMIT_PARAM_FIELD);
        total_size += obj.get_serialize_size();
        total_size += limit_.get_serialize_size();
      }
      if (has_filter_)
      {
        obj.set_ext(ObActionFlag::SQL_FILTER_PARAM_FIELD);
        total_size += obj.get_serialize_size();
        total_size += filter_.get_serialize_size();
      }

      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      total_size += obj.get_serialize_size();
      return total_size;
    }

    ObSqlReadParam& ObSqlReadParam::operator=(const ObSqlReadParam &other)
    {
      data_version_ = other.data_version_;
      is_read_master_ = other.is_read_master_;
      only_static_data_ = other.only_static_data_;
      is_result_cached_ = other. is_result_cached_;
      table_id_ = other.table_id_;
      renamed_table_id_ = other.renamed_table_id_;

      has_project_ = other.has_project_;
      if (other.has_project_)
      {
        project_.assign(&other.project_);
      }
      has_scalar_agg_ = other.has_scalar_agg_;
      if (other.has_scalar_agg_)
      {
        if (NULL == scalar_agg_)
        {
          scalar_agg_ = OB_NEW(ObScalarAggregate, ObModIds::OB_SQL_SCALAR_AGGR);
        }
        if (NULL == scalar_agg_)
        {
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          scalar_agg_->assign(other.scalar_agg_);
        }
      }
      has_group_ = other.has_group_;
      if (other.has_group_)
      {
        if (NULL == group_)
        {
          group_ = OB_NEW(ObMergeGroupBy, ObModIds::OB_SQL_MERGE_GROUPBY);
        }
        if (NULL == group_)
        {
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          group_->assign(other.group_);
        }
      }
      has_group_columns_sort_ = other.has_group_columns_sort_;
      if (other.has_group_columns_sort_)
      {
        group_columns_sort_.assign(&group_columns_sort_);
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      is_listagg_ = other.is_listagg_;
      if (other.is_listagg_)
      {
        sort_for_listagg_.assign(&sort_for_listagg_);
      }
      /* add 20150104:e */
      has_filter_ = other.has_filter_;
      if (other.has_filter_)
      {
        filter_.assign(&other.filter_);
      }
      has_limit_ = other.has_limit_;
      if (other.has_limit_)
      {
        limit_.assign(&other.limit_);
      }
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      data_mark_param_ = other.data_mark_param_;
      //add duyr 20160531:e
      this->set_phy_plan(const_cast<ObSqlReadParam*>(&other)->project_.get_phy_plan());
      return *this;
    }

    int64_t ObSqlReadParam::to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      //mod dragon 2016-9-6 16:44:01
      if (OB_INVALID_ID != table_id_)
      /*--old---
      if (OB_INVALID_ID == table_id_)
      ---old---*/
      //mod 2016-9-6e
      {
        databuff_printf(buf, buf_len, pos, "tid=%lu, ", table_id_);
      }
      else
      {
        databuff_printf(buf, buf_len, pos, "tid=NULL, ");
      }
      databuff_printf(buf, buf_len, pos, "is_indexed_group_=%d, ", is_indexed_group_);//add liumz, [optimize group_order by index]20170419
      //modify yushengjuan [ocanbase_gui_client] 20160218  add pos = pos - 1
      if (has_limit_)
      {
        databuff_print_obj(buf, buf_len, pos, limit_);
        pos = pos - 1;
      }
      if (has_scalar_agg_)
      {
        databuff_print_obj(buf, buf_len, pos, *scalar_agg_);
        pos = pos - 1;
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      if (is_listagg_)
      {
        databuff_print_obj(buf, buf_len, pos, sort_for_listagg_);
        pos = pos - 1;
      }
      /* add 20150104:e */
      if (has_group_)
      {
        databuff_print_obj(buf, buf_len, pos, *group_);
        pos = pos - 1;
      }
      if (has_group_columns_sort_ && !is_indexed_group_)//mod liumz, [optimize group_order by index]20170419
      {
        databuff_print_obj(buf, buf_len, pos, group_columns_sort_);
        pos = pos - 1;
      }

      if (has_filter_)
      {
        databuff_print_obj(buf, buf_len, pos, filter_);
        pos = pos - 1;
      }
      if (has_project_)
      {
        databuff_print_obj(buf, buf_len, pos, project_);
        pos = pos - 1;
      }
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      if (data_mark_param_.is_valid())
//      {
//        databuff_print_obj(buf, buf_len, pos, data_mark_param_);
//        pos = pos - 1;
//      }
      //add duyr 20160531:e
    //del by maosy
      //mod:e 20160218
      return pos;
    }

    int ObSqlReadParam::assign(const ObSqlReadParam* other)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObSqlReadParam);
      is_read_master_ = o_ptr->is_read_master_;
      is_result_cached_ = o_ptr->is_result_cached_;
      data_version_ = o_ptr->data_version_;
      table_id_ = o_ptr->table_id_;
      renamed_table_id_ = o_ptr->renamed_table_id_;
      only_static_data_ = o_ptr->only_static_data_;
      if (ret == OB_SUCCESS && o_ptr->has_project_)
      {
        has_project_ = o_ptr->has_project_;
        if ((ret = project_.assign(&o_ptr->project_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObProject failed, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && o_ptr->has_scalar_agg_)
      {
        has_scalar_agg_ = o_ptr->has_scalar_agg_;
        if (!scalar_agg_)
        {
          scalar_agg_ = OB_NEW(ObScalarAggregate, ObModIds::OB_SQL_MERGE_GROUPBY);
          if (scalar_agg_)
          {
            scalar_agg_->set_phy_plan(project_.get_phy_plan());
          }
          else
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "failed to alloc memory");
          }
        }
        if (ret == OB_SUCCESS && (ret = scalar_agg_->assign(o_ptr->scalar_agg_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObScalarAggregate failed, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && o_ptr->has_group_)
      {
        has_group_ = o_ptr->has_group_;
        if (!group_)
        {
          group_ = OB_NEW(ObMergeGroupBy, ObModIds::OB_SQL_MERGE_GROUPBY);
          if (group_)
          {
            group_->set_phy_plan(project_.get_phy_plan());
          }
          else
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "failed to alloc memory");
          }
        }
        if (ret == OB_SUCCESS && (ret = group_->assign(o_ptr->group_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObMergeGroupBy failed, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && o_ptr->has_group_columns_sort_)
      {
        has_group_columns_sort_ = o_ptr->has_group_columns_sort_;
        if ((ret = group_columns_sort_.assign(&o_ptr->group_columns_sort_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObSort failed, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && o_ptr->has_group_columns_sort_)
      {
        has_group_columns_sort_ = o_ptr->has_group_columns_sort_;
        if ((ret = group_columns_sort_.assign(&o_ptr->group_columns_sort_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObSort failed, ret=%d", ret);
        }
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      if (ret == OB_SUCCESS && o_ptr->is_listagg_)
      {
        is_listagg_ = o_ptr->is_listagg_;
        if ((ret = sort_for_listagg_.assign(&o_ptr->sort_for_listagg_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObSort failed, ret=%d", ret);
        }
      }
      /* add 20150104:e */
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      if (OB_SUCCESS == ret)
//      {
//          data_mark_param_ = o_ptr->data_mark_param_;
//      }
      //add duyr 20160531:e
    //del by maosy
      if (ret == OB_SUCCESS && o_ptr->has_limit_)
      {
        has_limit_ = o_ptr->has_limit_;
        if ((ret = limit_.assign(&o_ptr->limit_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign ObLimit failed, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && o_ptr->has_filter_)
      {
        has_filter_ = o_ptr->has_filter_;
        if ((ret = filter_.assign(&o_ptr->filter_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Assign filter_ failed, ret=%d", ret);
        }
      }
      return ret;
    }
	//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b 
	void ObSqlReadParam::reset_stuff()
	{
	  filter_.reuse();
      project_.reset_stuff();
	}
	//add 20140911:e
    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151206:b
    /*Exp:reset*/
    void ObSqlReadParam::reset_stuff_for_ud()
    {
      filter_.reuse();
    }
     //add 20151206:e

  } /* sql */
} /* oceanbase */
