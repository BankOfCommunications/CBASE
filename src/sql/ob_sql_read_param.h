#ifndef OCEANBASE_SQL_READ_PARAM_H_
#define OCEANBASE_SQL_READ_PARAM_H_

#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_rowkey.h"
#include "common/ob_scan_param.h"
#include "common/ob_string_buf.h"
#include "ob_project.h"
#include "ob_limit.h"
#include "ob_filter.h"
#include "ob_scalar_aggregate.h"
#include "ob_merge_groupby.h"
#include "ob_sort.h"


namespace oceanbase
{
  namespace sql
  {
    class ObSqlReadParam
    {
    public:
      ObSqlReadParam();
      virtual ~ObSqlReadParam();
      void reset(void);
      // basic field
      virtual inline void set_is_read_consistency(const bool cons);
      virtual inline bool get_is_read_consistency()const;
      virtual inline void set_is_only_static_data(bool only_static_data);
      virtual inline bool get_is_only_static_data()const;
      virtual inline void set_is_result_cached(const bool cached);
      virtual bool get_is_result_cached()const;
      virtual inline void set_data_version(int64_t data_version);
      virtual inline int64_t get_data_version() const;
      virtual inline int set_table_id(const uint64_t& renamed_table_id, const uint64_t& table_id);
      virtual inline uint64_t get_renamed_table_id() const;
      virtual inline uint64_t get_table_id() const;
      // operator fields
      virtual int set_project(const ObProject &project);
      virtual int add_output_column(const ObSqlExpression& expr);
      virtual int set_group_columns_sort(const ObSort &sort);
      virtual int set_filter(const ObFilter &filter);
      virtual int add_filter(ObSqlExpression *cond);
      virtual int add_group_column(const uint64_t tid, const uint64_t cid);
      virtual int add_aggr_column(const ObSqlExpression& expr);
      virtual int set_limit(const ObLimit &limit);
      virtual int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);
      virtual void set_phy_plan(ObPhysicalPlan *the_plan);
      virtual const ObProject &get_project() const;
      virtual const ObScalarAggregate &get_scalar_agg() const;
      virtual const ObMergeGroupBy &get_group() const;
      virtual const ObSort &get_group_columns_sort() const;
      virtual const ObSort &get_listagg_columns_sort() const;//add gaojt [ListAgg][JHOBv0.1]20150109
      virtual const ObFilter &get_filter() const;
      virtual const ObLimit &get_limit() const;
      virtual inline bool has_project() const;
      virtual inline bool has_scalar_agg() const;
      virtual inline bool has_group() const;
      virtual inline bool has_group_columns_sort() const;
      virtual inline bool has_filter() const;
      virtual inline bool has_limit() const;
      virtual inline int64_t get_output_column_size() const;
      // caution: NOT deep copy
      virtual ObSqlReadParam& operator=(const ObSqlReadParam &other);
      virtual int64_t to_string(char *buf, const int64_t buf_len) const;

      virtual int assign(const ObSqlReadParam* other);
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      void set_is_listagg(bool is_listagg){is_listagg_ = is_listagg;}
      virtual inline bool has_listagg() const;
      int add_sort_column_for_listagg(const uint64_t tid, const uint64_t cid,bool is_asc);
      /* add 20150104:e */
	  //add liumz, [optimize group_order by index]20170419:b
      void set_indexed_group(bool indexed){is_indexed_group_ = indexed;}
      bool is_indexed_group() const{return is_indexed_group_;}
	  //add:e
      VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
	  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140409:b

      /*Exp:get ObSqlExpression from readparam
      * @param idx[in] idx, sub_query's number in main_query
      * @param index[out] expr, ObSqlExpression which has corresponding sub_query
      * @param index[out] query_index, sub_query's order in current expression
      */
      int get_param_sub_query_expr(int32_t idx,ObSqlExpression* &expr,int &query_index){return filter_.get_sub_query_expr(idx,expr,query_index);};
	  
	  /*Exp:remove expression which has sub_query but not use bloomfilter 
	  * make stmt pass the first check 
	  */
      int remove_or_sub_query_expr();
	  
      /*Exp:update sub_query num
      * if direct bind sub_query result to main query,
      * do not treat it as a sub_query any more
      */
      void update_sub_query_num();
	  //add 20140409:e
      //add fanqiushi_index
      int reset_project_and_filter();
      //add:e
      //add wanglei [semi join] 20160111
      int reuse_project_and_filter();
      //add:e
      //add wanglei [semi join update] 20160415:b
      ObFilter & get_raw_filter();
      //add wanglei [semi join update] 20160415:e
      void reset_stuff();//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911 /*Exp:reset*/
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151206:b
      /*Exp:reset*/
      void reset_stuff_for_ud();
       //add 20151206:e
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      virtual void set_data_mark_param(const ObDataMarkParam &param)
//      {
//          data_mark_param_ = param;
//      }
//      virtual ObDataMarkParam& get_data_mark_param()
//      {
//          return data_mark_param_;
//      }
//      virtual const ObDataMarkParam& get_data_mark_param()const
//      {
//          return data_mark_param_;
//      }
      //add duyr 20160531:e
//del by maosy
    protected:
      // RESERVE_PARAM_FIELD
      int serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_reserve_param_serialize_size(void) const;
      int serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const;

    private:
      int serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_basic_param_serialize_size(void) const;
    protected:
      int8_t is_read_master_;
      int8_t is_result_cached_;
      int64_t data_version_;
      uint64_t table_id_;
      uint64_t renamed_table_id_;
      bool only_static_data_;

      ObProject project_;
      ObScalarAggregate *scalar_agg_;
      ObMergeGroupBy *group_;
      ObSort group_columns_sort_;
      ObLimit limit_;
      ObFilter filter_;
      bool has_project_;
      bool has_scalar_agg_;
      bool has_group_;
      bool has_group_columns_sort_;
      bool is_indexed_group_;//add liumz, [optimize group_order by index]20170419
      bool has_limit_;
      bool has_filter_;
      bool is_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
      ObSort sort_for_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      ObDataMarkParam data_mark_param_;
      //add duyr 20160531:e
    //del by maosy
    };

    inline void ObSqlReadParam::set_is_only_static_data(bool only_static_data)
    {
      only_static_data_ = only_static_data;
    }

    inline bool ObSqlReadParam::get_is_only_static_data() const
    {
      return only_static_data_;
    }

    inline void ObSqlReadParam::set_is_read_consistency(const bool consistency)
    {
      is_read_master_ = consistency;
    }

    inline bool ObSqlReadParam::get_is_read_consistency()const
    {
      return (is_read_master_ > 0);
    }

    inline void ObSqlReadParam::set_is_result_cached(const bool cached)
    {
      is_result_cached_ = cached;
    }

    inline bool ObSqlReadParam::get_is_result_cached()const
    {
      return (is_result_cached_ > 0);
    }

    inline void ObSqlReadParam::set_data_version(int64_t data_version)
    {
      data_version_ = data_version;
    }

    inline int64_t ObSqlReadParam::get_data_version() const
    {
      return data_version_;
    }

    int ObSqlReadParam::set_table_id(const uint64_t& renamed_table_id, const uint64_t& table_id)
    {
      int err = OB_SUCCESS;
      renamed_table_id_ = renamed_table_id;
      table_id_ = table_id;
      return err;
    }

    inline uint64_t ObSqlReadParam::get_renamed_table_id() const
    {
      return renamed_table_id_;
    }

    inline uint64_t ObSqlReadParam::get_table_id() const
    {
      return table_id_;
    }

    inline bool ObSqlReadParam::has_project() const
    {
      return has_project_;
    }

    inline bool ObSqlReadParam::has_filter() const
    {
      return has_filter_;
    }

    inline bool ObSqlReadParam::has_scalar_agg() const
    {
      return has_scalar_agg_;
    }

    inline bool ObSqlReadParam::has_group() const
    {
      return has_group_;
    }

    inline bool ObSqlReadParam::has_group_columns_sort() const
    {
      return has_group_columns_sort_;
    }

    inline bool ObSqlReadParam::has_limit() const
    {
      return has_limit_;
    }

    inline int64_t ObSqlReadParam::get_output_column_size() const
    {
      return project_.get_output_column_size();
    }

    inline const ObScalarAggregate & ObSqlReadParam::get_scalar_agg() const
    {
      return *scalar_agg_;
    }

    inline const ObMergeGroupBy & ObSqlReadParam::get_group() const
    {
      return *group_;
    }

    inline const ObSort & ObSqlReadParam::get_group_columns_sort() const
    {
      return group_columns_sort_;
    }
    //add  [ListAgg][JHOBv0.1]20150109:b
    inline bool ObSqlReadParam::has_listagg() const
    {
      return is_listagg_;
    }
    inline const ObSort & ObSqlReadParam::get_listagg_columns_sort() const
    {
      return sort_for_listagg_;
    }
    //add 20150109:e
  } /* sql */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_SQL_READ_PARAM_H_ */
