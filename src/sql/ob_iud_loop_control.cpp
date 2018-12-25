//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
/**
 * ob_bind_values.cpp
 *
 * Authors:
 *   gaojt
 *function:Insert_Subquery_Function
 * used for insert ... select
 */

#include "ob_iud_loop_control.h"
#include "ob_bind_values.h"
#include "ob_ups_executor.h"
#include "ob_physical_plan.h"
#include "ob_sql_session_info.h"
//add maosy [Delete_Update_Function] 20170106:b
#include "common/ob_errno.h"
//add:e
//del by maosy [Delete_Update_Function]
//bool is_multi_batch_over = false;
//del e
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObIudLoopControl::ObIudLoopControl():
// add by maosy  [Delete_Update_Function]
 //batch_num_(0),inserted_row_num_(0),is_multi_batch_(false)
  batch_num_(0),affected_row_(0),threshold_(0),need_start_trans_(false)/*,is_ud_(false)*/
  // add e
{
}

ObIudLoopControl::~ObIudLoopControl()
{
}

void ObIudLoopControl::reset()
{
    // add by maosy [Delete_Update_Function]
  affected_row_=0;
  need_start_trans_ =false;
  // add e
//  is_ud_ = false;
}
//add by gaojt [Delete_Update_Function-Transaction] 201601206:b
int ObIudLoopControl::commit()
{
  TBSYS_LOG(DEBUG,"enter into commit()");
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("COMMIT");
  ret = execute_stmt_no_return_rows(start_thx);
  TBSYS_LOG(DEBUG,"leave commit()");
  return ret;
}

int ObIudLoopControl::start_transaction()
{
  TBSYS_LOG(DEBUG,"enter into start_transaction()");
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("START TRANSACTION");
  ret = execute_stmt_no_return_rows(start_thx);
  TBSYS_LOG(DEBUG,"leave start_transaction()");
  return ret;
}

int ObIudLoopControl::execute_stmt_no_return_rows(const ObString &stmt)
{
  TBSYS_LOG(DEBUG,"enter into execute_stmt_no_return_rows()");
  int ret = OB_SUCCESS;
  ObMySQLResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, sql_context_)))
  {
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else
  {
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  TBSYS_LOG(DEBUG,"leave execute_stmt_no_return_rows()");
  return ret;
}

int ObIudLoopControl::rollback()
{
    TBSYS_LOG(DEBUG,"enter into rollback()");
    int ret = OB_SUCCESS;
    ObString start_thx = ObString::make_string("ROLLBACK");
    ret = execute_stmt_no_return_rows(start_thx);
    TBSYS_LOG(DEBUG,"leave rollback()");
    return ret;
}
//add by gaojt 201601206:e
void ObIudLoopControl::reuse()
{

}
int ObIudLoopControl::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  int ret = OB_SUCCESS;
  return ret;
}

//never used
int ObIudLoopControl::get_next_row(const ObRow *&row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  return ret;
}
/*Exp:for the explain stm*/
int64_t ObIudLoopControl::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObIudLoopControl(\n");
  databuff_printf(buf, buf_len, pos, ")\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

/*Exp:close the sub-operator of ObIudLoopControl*/
int ObIudLoopControl::close()
{
  // add by maosy [Delete_Update_Function]
  affected_row_=0;
  // add e
  return ObSingleChildPhyOperator::close();
}

int ObIudLoopControl::open()
{
    int ret = OB_SUCCESS;
    bool is_ud_original = false;//标记量，标记是批量还是单条，单条为true
    ObFillValues* fill_values = NULL;
    ObBindValues* bind_values = NULL;
    get_fill_bind_values(fill_values,bind_values);
    //add by gaojt [Delete_Update_Function-Transaction] 201601206:b
    bool is_start_transaction = false;//标记这个事务是否是我手动开启
    if(NULL == fill_values && NULL == bind_values)
    {
        is_ud_original = true;
    }
   // mod by maosy [Delete_Update_Function_isolation_RC] 20161218
    if (!is_ud_original && (need_start_trans_  || !sql_context_.session_info_->get_autocommit ()))
        //单条语句不开启事务，只有是批量的时候才会开启事务
  //if (is_ud_)
  //mod e
    {
        if (sql_context_.session_info_->get_trans_id().is_valid())
        {
           is_start_transaction = false;//已经存在事务，不需要我手动开启
        }
        else if (OB_SUCCESS != (ret = start_transaction()))
        {
            TBSYS_LOG(WARN,"fail to start transaction");
        }
        else
        {
            is_start_transaction = true;//事务手动开启
        }
    }
    // add by maosy [Delete_Update_Function_isolation_RC] 20170220
    if(!is_ud_original && sql_context_.session_info_->get_trans_id().is_valid() && fill_values !=NULL)
    {
        fill_values->set_need_to_tage_empty_row (true);
    }
        //add e
    //add by gaojt 201601206:e
    if(OB_SUCCESS != ret )
    {}
    else if(1 != get_child_num())
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "the operator ObIudLoopControl is not init");
    }
    // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
//    else if(!is_multi_batch_)
//    {
//        if (NULL == get_child(0))
//        {
//            ret = OB_ERR_UNEXPECTED;
//            TBSYS_LOG(ERROR, "Ups Executor must not be NULL");
//        }
//        else
//        {
//            ret = get_child(0)->open();
//            if(OB_ITER_END == ret)
//            {
//                ret = OB_SUCCESS;
//            }
//            else if (OB_SUCCESS != ret)
//            {
//                if (OB_SUCCESS != (rollback()))
//                {
//                    TBSYS_LOG(WARN,"fail to rollback");
//                }
//                else
//                {
//                    ret = OB_TRANS_ROLLBACKED ;
//                    ObTransID invalid_trans;
//                    sql_context_.session_info_->set_trans_id (invalid_trans);
//                    TBSYS_LOG(WARN, "failed to open Ups Executor,transaction is rollback , ret=%d", ret);
//                }
//            }
//            if(ret ==OB_SUCCESS)
//            {
//            // add by maosy [Delete_Update_Function]
//             get_affect_rows(is_ud_original,threshold_,fill_values,bind_values);
//             // add e

//            }
//        }
//    }
// mod by maosy
    else
    {
        //del by maosy [Delete_Update_Function]
        //is_multi_batch_over = false;
     //while(OB_SUCCESS == ret && !is_multi_batch_over)
        while(OB_SUCCESS == ret && !is_batch_over(fill_values,bind_values))
        {
     //del e

            if (NULL == get_child(0))
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(ERROR, "Ups Executor must not be NULL");
            }
            else
            {
                ret = get_child(0)->open();
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(WARN, "failed to open Ups Executor , ret=%d", ret);
                }
                // add by maosy [Delete_Update_Function]
                else
                {
                get_affect_rows(is_ud_original,threshold_,fill_values,bind_values);
                batch_num_++;
                }
            }
            if (is_ud_original)
            {
                break;
            }
            // add by maosy  [Delete_Update_Function] 20160619:b
            else
            {
            TBSYS_LOG(DEBUG,"has manipulation %ld rows , %ld batch is over ",affected_row_,batch_num_);
            // add by maosy 20160619:e
            }
        }
        if(OB_ITER_END == ret)
        {
            ret = OB_SUCCESS;
        }
        //del by maosy 20170220
//        else if(OB_SUCCESS != ret)
//        {
//    // mod by maosy [Delete_Update_Function]
//     //TBSYS_LOG(WARN, "This is the %ld batch data,and you have inserted %ld rows",batch_num_,inserted_row_num_);
//            TBSYS_LOG(WARN, "This is the %ld batch data,has handled %ld rows, and the threshhold is %ld",
//                      batch_num_,affected_row_,threshold_);
//      // mod e
//            if (NULL != fill_values && !fill_values->is_already_clear())
//            {
//                fill_values->clear_prepare_select_result();
//            }
//        }
        // del e
    }
  // add by maosy  [Delete_Update_Function]20170220
    if (!is_ud_original)

    {
        set_affect_row();
        sql_context_.session_info_->set_read_times (NO_BATCH_UD);
        //如果是批量，并且执行失败了， 不管什么错，都需要回滚事务
        if(OB_SUCCESS == ret)
        {}
        else
        {
            TBSYS_LOG(WARN, "This is the %ld batch data,has handled %ld rows, and the threshhold is %ld",
                      batch_num_,affected_row_,threshold_);
            if (NULL != fill_values && !fill_values->is_already_clear())
            {
                fill_values->clear_prepare_select_result();
            }
            if( OB_SUCCESS != (rollback()))
            {
                TBSYS_LOG(WARN,"fail to rollback");
            }
            else
            {   //mod maosy [Delete_Update_Function] 20170106:b
                TBSYS_LOG(USER_ERROR,"%s, error code = %d", ob_strerror(ret), ret);
                //mod:e
                ret = OB_TRANS_ROLLBACKED;
                TBSYS_LOG(INFO,"transaction is rolled back");
            }
        }
    }
// add e
    //add by gaojt [Delete_Update_Function-Transaction] 201601206:b
    // 如果事务时我开启，并且是自动提交的事务，那我需要在执行完成后提交事务
    if (is_start_transaction && sql_context_.session_info_->get_autocommit ())
    {
        if ( OB_SUCCESS == ret)
        {
            if (OB_SUCCESS != (ret = commit()))
            {
                TBSYS_LOG(WARN,"fail to commit");
            }
        }
    // add by maosy [Delete_Update_Function_isolation_RC] 20161228
//        else if (OB_SUCCESS != (rollback()))
//        {
//            TBSYS_LOG(WARN,"fail to rollback");
//        }
    //del e
    }
    //add by gaojt 201601206:e

    return ret;
}

void ObIudLoopControl::get_affect_rows(bool& is_ud_original,
                                       int64_t& threshhold,
                                       ObFillValues* fill_values,
                                       ObBindValues* bind_values)
{
    if (NULL != fill_values)
    {
        affected_row_+=fill_values->get_affect_row();
        fill_values->get_threshhold(threshhold);
    }
    else if (NULL != bind_values)
    {
        affected_row_+=bind_values->get_inserted_row_num();
        bind_values->get_threshhold(threshhold);
    }
    else
    {
        is_ud_original = true;
    }
}

// add by maosy [Delete_Update_Function]
void ObIudLoopControl::set_affect_row()
{
    OB_ASSERT(my_phy_plan_);
    ObResultSet* outer_result_set = NULL;
    ObResultSet* my_result_set = NULL;
    my_result_set = my_phy_plan_->get_result_set();
    outer_result_set = my_result_set->get_session()->get_current_result_set();
    my_result_set->set_session(outer_result_set->get_session()); // be careful!
    TBSYS_LOG(DEBUG,"set affected row =%ld",affected_row_);
    outer_result_set->set_affected_rows(affected_row_);
}
bool ObIudLoopControl::is_batch_over(ObFillValues*& fill_values,ObBindValues*& bind_values)
{
    bool is_over = false;
    if(NULL != fill_values)
    {
        is_over = fill_values->is_multi_batch_over();
    }
    else if (NULL != bind_values)
    {
        is_over = bind_values->is_batch_over();
    }
    return is_over;
}
// add e

void ObIudLoopControl::get_fill_bind_values(ObFillValues *&fill_values, ObBindValues *&bind_values)
{
    OB_ASSERT(my_phy_plan_);
    int64_t num = my_phy_plan_->get_operator_size();
    for(int64_t i = 0 ; i <num ; i++)
    {
        ObUpsExecutor* ups_executor= dynamic_cast<ObUpsExecutor*>(my_phy_plan_->get_phy_operator(i));
        if(ups_executor!=NULL)
        {
            ObPhysicalPlan * inner_plan = ups_executor->get_inner_plan();
            int64_t ups_num = inner_plan->get_operator_size();
            for(int64_t j = 0 ; j <ups_num ; j++)
            {
                ObPhyOperator* phy_opt = inner_plan->get_phy_operator(j);
                if(NULL !=(dynamic_cast<ObFillValues*>(phy_opt)))
                {
                    fill_values= dynamic_cast<ObFillValues*>(phy_opt);
                    break;
                }
                else if(NULL !=(dynamic_cast<ObBindValues*>(phy_opt)))
                {
                    bind_values= dynamic_cast<ObBindValues*>(phy_opt);
                    break;
                }

            }//end for
            break;
        }//end if
    }//end for
}

PHY_OPERATOR_ASSIGN(ObIudLoopControl)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObIudLoopControl);
  reset();
  batch_num_ = o_ptr->batch_num_;
  // mod by maosy  [Delete_Update_Function]
//    inserted_row_num_ = o_ptr->inserted_row_num_;
  affected_row_ = o_ptr->affected_row_;
  // mod e
  return ret;
}
namespace oceanbase{
namespace sql
{
REGISTER_PHY_OPERATOR(ObIudLoopControl,PHY_IUD_LOOP_CONTROL);
}
}
//add gaojt 20150507:e
