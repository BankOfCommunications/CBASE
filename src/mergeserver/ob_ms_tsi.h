#ifndef OCEANBASE_MERGESERVER_OB_MS_TSI_H_
#define OCEANBASE_MERGESERVER_OB_MS_TSI_H_
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_postfix_expression.h"
#include "common/hash/ob_hashutils.h"
#include <string.h>
namespace oceanbase
{
  namespace mergeserver
  {
    using oceanbase::common::OB_SUCCESS;
    using oceanbase::common::OB_ERROR;
    static const int32_t ORG_PARAM_ID = 1;
    static const int32_t DECODED_PARAM_ID = 2;
    static const int32_t RESULT_SCANNER_ID = 3;
    static const int32_t SCHEMA_DECODER_ASSIS_ID = 4;
    static const int32_t UPS_SCANNER_ID = 5;
    static const int32_t SERVER_COUNTER_ID = 6; 
    struct ObMSSchemaDecoderAssis
    {
    public:
      typedef common::hash::ObHashMap<common::ObString, int64_t, common::hash::NoPthreadDefendMode> HashMap;
      static const int32_t INVALID_IDX = -1;
      ObMSSchemaDecoderAssis()
      {
        inited_ = false;
      }
      inline int init()
      {
        int err = OB_SUCCESS;

        if ((OB_SUCCESS == err) && !inited_
          && (OB_SUCCESS != (err = select_cname_to_idx_map_.create(hash_map_bucket_num_))))
        {
          TBSYS_LOG(WARN,"fail to create hash map for select column name to column index map [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && !inited_
          && (OB_SUCCESS != (err = groupby_cname_to_idx_map_.create(hash_map_bucket_num_))))
        {
          TBSYS_LOG(WARN,"fail to create hash map for groupby column name to column index map [err:%d]", err);
        }
        if(OB_SUCCESS == err)
        {
          inited_ = true;
        }
        return err;
      }

      inline void clear()
      {
        for (int32_t i = 0; i < oceanbase::common::OB_MAX_COLUMN_NUMBER; i++)
        {
          column_idx_in_org_param_[i] = INVALID_IDX;
        }
        if (inited_)
        {
          groupby_cname_to_idx_map_.clear();
          select_cname_to_idx_map_.clear();
        }
      }

      enum
      {
        GROUPBY_COLUMN,
        SELECT_COLUMN
      };
      inline int add_column(const oceanbase::common::ObString &cname, const int64_t idx, 
        int column_type, int overwrite_flag = 0)
      {
        int err = OB_SUCCESS;
        int hash_err = 0;
        HashMap *target = NULL;
        if (GROUPBY_COLUMN == column_type)
        {
          target = &groupby_cname_to_idx_map_;
        }
        else if (SELECT_COLUMN == column_type)
        {
          target = &select_cname_to_idx_map_;
        }
        else
        {
          TBSYS_LOG(WARN,"unknow column type [type:%d]", column_type);
          err = oceanbase::common::OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS == err)
        {
          if (oceanbase::common::hash::HASH_INSERT_SUCC == 
            (hash_err =  target->set(cname,idx, overwrite_flag)))
          {
            /// do nothing
          }
          else if (oceanbase::common::hash::HASH_OVERWRITE_SUCC  == hash_err)
          {
            /// do nothing
          }
          else if (oceanbase::common::hash::HASH_EXIST == hash_err)
          {
            err = oceanbase::common::OB_ENTRY_EXIST;
          }
          else
          {
            TBSYS_LOG(WARN,"fail to add column <cname,idx> pair [err:%d]", hash_err);
            err = OB_ERROR;
          }
        }
        return err;
      }

      inline const HashMap & get_select_cname_to_idx_map()const
      {
        return select_cname_to_idx_map_;
      }
      inline const HashMap & get_groupby_cname_to_idx_map()const
      {
        return groupby_cname_to_idx_map_;
      }

      inline oceanbase::common::ObExpressionParser & get_post_expression_parser()
      {
        return post_expression_parser_;
      }

      int64_t column_idx_in_org_param_[oceanbase::common::OB_MAX_COLUMN_NUMBER];
    private:
      bool inited_;
      static const int64_t hash_map_bucket_num_ = 1024*2;
      HashMap select_cname_to_idx_map_;
      HashMap groupby_cname_to_idx_map_;
      oceanbase::common::ObExpressionParser post_expression_parser_;
    };
  }
}


#endif /* OCEANBASE_MERGESERVER_OB_MS_TSI_H_ */
