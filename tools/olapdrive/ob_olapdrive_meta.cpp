/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_meta.h for define olapdrive meta table 
 * operation. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "common/ob_atomic.h"
#include "common/murmur_hash.h"
#include "common/ob_tsi_factory.h"
#include "common/file_utils.h"
#include "common/file_directory_utils.h"
#include "common/utility.h"
#include "ob_olapdrive_meta.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace common;
    using namespace hash;
    using namespace serialization;

    void ObKeyMeta::reset()
    {
      memset(this, 0, sizeof(ObKeyMeta));
    }

    bool ObKeyMeta::is_valid()
    {
      return !(max_unit_id_ <= 0 || min_date_ <= 0 || max_date_ <= 0
               || max_campaign_id_ <= 0 || max_adgroup_id_ <= 0
               || max_key_id_ <= 0 || max_creative_id_ <= 0
               || daily_unit_count_ <= 0);
    }

    void ObKeyMeta::display() const
    {
      fprintf(stderr, "\tmax_unit_id_: %ld \n"
                      "\tmin_date_: %ld \n"
                      "\tmax_date_: %ld \n"
                      "\tmax_campaign_id_: %ld \n"
                      "\tmax_adgroup_id_: %ld \n"
                      "\tmax_key_id_: %ld \n"
                      "\tmax_creative_id_: %ld \n"
                      "\tdaily_unit_count_: %ld \n", 
              max_unit_id_, min_date_, max_date_,
              max_campaign_id_, max_adgroup_id_,
              max_key_id_, max_creative_id_,
              daily_unit_count_);
    }

    void ObCampaign::reset()
    {
      memset(this, 0, sizeof(ObCampaign));
    }

    void ObCampaign::display() const
    {
      fprintf(stderr, "\tcampaign_id_: %ld \n"
                      "\tstart_adgroup_id_: %ld \n"
                      "\tend_adgroup_id_: %ld \n"
                      "\tstart_id_: %ld \n"
                      "\tend_id_: %ld \n",
              campaign_id_, start_adgroup_id_, 
              end_adgroup_id_, start_id_, end_id_);
    }

    void ObAdgroup::reset()
    {
      memset(this, 0, sizeof(ObAdgroup));
    }

    void ObAdgroup::display() const
    {
      fprintf(stderr, "\tcampaign_id_: %ld \n"
                      "\tadgroup_id_: %ld \n"
                      "\tstart_id_: %ld \n"
                      "\tend_id_: %ld \n",
              campaign_id_, adgroup_id_, start_id_, end_id_);
    }

    ObOlapdriveMeta::ObOlapdriveMeta(ObOlapdriveSchema& olapdrive_schema,
                                     client::ObClient& client)
    : inited_(false), client_(client), olapdrive_schema_(olapdrive_schema)
    {

    }

    ObOlapdriveMeta::~ObOlapdriveMeta()
    {

    }

    int ObOlapdriveMeta::init()
    {
      int ret = OB_SUCCESS;

      ret = init_rowkey_meta();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to init rowkey meta");
      }
      else
      {
        ret = campaign_hash_map_.create(CAMPAIGN_HASH_SLOT_NUM);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to create campaign hash table");
        }
        else
        {
          ret = adgroup_hash_map_.create(ADGROUP_HASH_SLOT_NUM);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to create adgroup hash table");
          }
        }
      }

      return ret;
    }
    
    void ObOlapdriveMeta::set_rowkey_meta(ObKeyMeta& key_meta)
    {
      key_meta_ = key_meta;
    }

    const ObKeyMeta& ObOlapdriveMeta::get_rowkey_meta() const
    {
      return key_meta_;
    }

    ObKeyMeta& ObOlapdriveMeta::get_rowkey_meta()
    {
      return key_meta_;
    }

    int ObOlapdriveMeta::get_cell_value(const ObCellInfo& cell_info, 
                                        int64_t& value)
    {
      int ret             = OB_SUCCESS;
      ObObjType cell_type = cell_info.value_.get_type();

      if (OB_SUCCESS == ret)
      {
        switch (cell_type)
        {
        case ObIntType:
          ret = cell_info.value_.get_int(value);
          break;

        case ObExtendType:
          if (cell_info.value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
          {
            TBSYS_LOG(WARN, "row doesn't exist for get rowkey range");
            ret = OB_ERROR;
          }
          break;

        case ObFloatType:
        case ObDoubleType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        case ObVarcharType:
        case ObCreateTimeType:
        case ObModifyTimeType:
        case ObNullType:
        case ObSeqType:
        default:
          TBSYS_LOG(WARN, "invalid cell info type %d", cell_type);
          ret = OB_ERROR;
          break;
        }
      }

      return ret;
    }

    int ObOlapdriveMeta::read_rowkey_meta(ObScanner& scanner)
    {
      int ret         = OB_SUCCESS;
      int64_t index   = 0;
      ObScannerIterator iter;
      ObCellInfo cell_info;

      for (iter = scanner.begin(); 
           iter != scanner.end() && OB_SUCCESS == ret; 
           iter++, index++)
      {
        cell_info.reset();
        ret = iter.get_cell(cell_info);
        if (OB_SUCCESS == ret)
        {
          if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info.value_.get_ext())
          {
            break;
          }

          switch (index)
          {
          case 0:
            ret = get_cell_value(cell_info, key_meta_.max_unit_id_);
            break;

          case 1:
            ret = get_cell_value(cell_info, key_meta_.min_date_);
            break;

          case 2:
            ret = get_cell_value(cell_info, key_meta_.max_date_);
            break;

          case 3:
            ret = get_cell_value(cell_info, key_meta_.max_campaign_id_);
            break;

          case 4:
            ret = get_cell_value(cell_info, key_meta_.max_adgroup_id_);
            break;

          case 5:
            ret = get_cell_value(cell_info, key_meta_.max_key_id_);
            break;

          case 6:
            ret = get_cell_value(cell_info, key_meta_.max_creative_id_);
            break;

          case 7:
            ret = get_cell_value(cell_info, key_meta_.daily_unit_count_);
            break;

          default:
            TBSYS_LOG(WARN, "more than 8 cell returned for get rowkey meta");
            ret = OB_ERROR;
            break;
          }
        }
      }

      key_meta_.display();

      return ret;
    }

    void ObOlapdriveMeta::init_version_range(ObVersionRange& ver_range)
    {
      ver_range.start_version_ = 0;
      ver_range.border_flag_.set_inclusive_start();
      ver_range.border_flag_.set_max_value();
    }

    int ObOlapdriveMeta::init_rowkey_meta()
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* key_meta_schema  = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name               = NULL;
      int32_t column_size                   = 0 ;
      ObCellInfo cell_info;
      ObString table_name;
      ObObj rowkey_objs[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      ObRowkey row_key;
      ObVersionRange ver_range;

      ObScanner* scanner = GET_TSI_MULT(ObScanner,TSI_OLAP_SCANNER_1);
      ObGetParam* get_param = GET_TSI_MULT(ObGetParam,TSI_OLAP_GET_PARAM_1);

      if (NULL == scanner || NULL == get_param)
      {
        TBSYS_LOG(ERROR, "failed to get thread local get_param or scanner, "
                         "scanner=%p, get_param=%p", scanner, get_param);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        get_param->reset();
        scanner->reset();

        //the row with key 0 in key meta table stores row key meta
        rowkey_objs[0].set_int(0);
        row_key.assign (rowkey_objs, 1);
        table_name = olapdrive_schema_.get_key_meta_name();
  
        //build get_param
        key_meta_schema = olapdrive_schema_.get_key_meta_schema();
        if (NULL != key_meta_schema)
        {
          column_schema = olapdrive_schema_.get_schema_manager().
            get_table_schema(key_meta_schema->get_table_id(), column_size);
          for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
          {
            cell_info.reset();
            cell_info.table_name_ = table_name;
            cell_info.row_key_ = row_key;
            if (NULL != &column_schema[i])
            {
              column_name = column_schema[i].get_name();
              cell_info.column_name_.assign(const_cast<char*>(column_name), 
                                            static_cast<int32_t>(strlen(column_name)));
              ret = get_param->add_cell(cell_info);
            }
            else
            {
              ret = OB_ERROR;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "key meta table schema is invalid, key_meta_schema=%p", 
                    key_meta_schema);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        get_param->set_is_result_cached(true);
        init_version_range(ver_range);
        get_param->set_version_range(ver_range);
        ret = client_.ms_get(*get_param, *scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to get cell from merge server, rowkey: %s",
              to_cstring(row_key));
        }
      }     

      if (OB_SUCCESS == ret)
      {
        ret = read_rowkey_meta(*scanner);
      }
      
      return ret; 
    }

    int ObOlapdriveMeta::set_cell_value(ObObj& obj, const ObObjType type, 
                                        const int64_t value)
    {
      int ret = OB_SUCCESS;

      switch (type)
      {
      case ObIntType:
        obj.set_int(value);
        break;

      case ObFloatType:
      case ObDoubleType:
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

    int ObOlapdriveMeta::write_rowkey_meta()
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* key_meta_schema  = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name_str           = NULL;
      int32_t column_size                   = 0;
      ObString table_name;
      ObObj rowkey_objs[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      ObRowkey row_key;
      ObString column_name;
      ObObj obj;
      ObMutator* mutator = GET_TSI_MULT(ObMutator,TSI_OLAP_MUTATOR_1);

      if (NULL == mutator)
      {
        TBSYS_LOG(ERROR, "failed to get thread local mutator, mutator=%p", mutator);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        mutator->reset();

        //the row with key 0 in wide table stores row key range
        rowkey_objs[0].set_int(0);
        row_key.assign (rowkey_objs, 1);
        table_name = olapdrive_schema_.get_key_meta_name();
  
        //build mutator
        key_meta_schema = olapdrive_schema_.get_key_meta_schema();
        if (NULL != key_meta_schema)
        {
          column_schema = olapdrive_schema_.get_schema_manager().
            get_table_schema(key_meta_schema->get_table_id(), column_size);
          for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
          {
            if (NULL != &column_schema[i])
            {
              switch (i)
              {
              case 0:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.max_unit_id_);
                break;
  
              case 1:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.min_date_);
                break;
  
              case 2:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.max_date_);
                break;
  
              case 3:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.max_campaign_id_);
                break;
  
              case 4:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.max_adgroup_id_);
                break;
  
              case 5:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.max_key_id_);
                break;
  
              case 6:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.max_creative_id_);
                break;
  
              case 7:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     key_meta_.daily_unit_count_);
                break;
  
              default:
                TBSYS_LOG(WARN, "more than 8 cells to store for set rowkey meta");
                ret = OB_ERROR;
                break;
              }
  
              if (OB_SUCCESS == ret)
              {
                column_name_str = column_schema[i].get_name();
                column_name.assign(const_cast<char*>(column_name_str), 
                                   static_cast<int32_t>(strlen(column_name_str)));
                ret = mutator->update(table_name, row_key, column_name, obj);
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
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_.ups_apply(*mutator);
      }

      return ret;
    }

    int ObOlapdriveMeta::get_campaign(
        const int64_t unit_id, 
        const int64_t campaign_id,
        ObCampaign& campaign)
    {
      int ret = OB_SUCCESS;
      int hash_err = OB_SUCCESS;
      ObCampaignKey campaign_key(unit_id, campaign_id);

      hash_err = campaign_hash_map_.get(campaign_key, campaign);
      if (HASH_NOT_EXIST == hash_err)
      {
        ret = get_database_campaign(unit_id, campaign_id, campaign);
        if (OB_SUCCESS == ret)
        {
          hash_err = campaign_hash_map_.set(campaign_key, campaign);
          if (HASH_INSERT_SUCC != hash_err)
          {
            TBSYS_LOG(WARN, "failed to set campaign into hash map, hash_err=%d", 
                      hash_err);            
          }
        }
      }
      else if (HASH_EXIST == hash_err)
      {
        //get from hash map, do nothing
      }
      else
      {
        TBSYS_LOG(WARN, "failed to get campaign from hash map, hash_err=%d", 
                  hash_err);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObOlapdriveMeta::get_database_campaign(
        const int64_t unit_id, 
        const int64_t campaign_id,
        ObCampaign& campaign)
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* campaign_schema  = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name               = NULL;
      int32_t column_size                   = 0 ;
      ObCellInfo cell_info;
      ObString table_name;
      ObObj rowkey_objs[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      ObRowkey row_key;
      ObVersionRange ver_range;

      ObScanner* scanner = GET_TSI_MULT(ObScanner,TSI_OLAP_SCANNER_1);
      ObGetParam* get_param = GET_TSI_MULT(ObGetParam,TSI_OLAP_GET_PARAM_1);

      if (NULL == scanner || NULL == get_param)
      {
        TBSYS_LOG(ERROR, "failed to get thread local get_param or scanner, "
                         "scanner=%p, get_param=%p", scanner, get_param);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        get_param->reset();
        scanner->reset();

        //build rowkey
        rowkey_objs[0].set_int(unit_id);
        rowkey_objs[1].set_int(campaign_id);
        row_key.assign (rowkey_objs, 2);
      }

      if (OB_SUCCESS == ret)
      {
        table_name = olapdrive_schema_.get_campaign_name();
  
        //build get_param
        campaign_schema = olapdrive_schema_.get_campaign_schema();
        if (NULL != campaign_schema)
        {
          column_schema = olapdrive_schema_.get_schema_manager().
            get_table_schema(campaign_schema->get_table_id(), column_size);
          for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
          {
            cell_info.reset();
            cell_info.table_name_ = table_name;
            cell_info.row_key_ = row_key;
            if (NULL != &column_schema[i])
            {
              column_name = column_schema[i].get_name();
              cell_info.column_name_.assign(const_cast<char*>(column_name), 
                                            static_cast<int32_t>(strlen(column_name)));
              ret = get_param->add_cell(cell_info);
            }
            else
            {
              ret = OB_ERROR;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "campaign table schema is invalid, campaign_schema=%p", 
                    campaign_schema);
          ret = OB_ERROR;
        }
      }
  
      if (OB_SUCCESS == ret)
      {
        get_param->set_is_result_cached(true);
        init_version_range(ver_range);
        get_param->set_version_range(ver_range);
        ret = client_.ms_get(*get_param, *scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to get cell from merge server, rowkey: %s", 
              to_cstring(row_key));
        }
      }     

      if (OB_SUCCESS == ret)
      {
        ret = read_campaign(*scanner, campaign);
      }
      
      return ret; 
    }

    int ObOlapdriveMeta::read_campaign(ObScanner& scanner, ObCampaign& campaign)
    {
      int ret         = OB_SUCCESS;
      int64_t index   = 0;
      ObScannerIterator iter;
      ObCellInfo cell_info;

      for (iter = scanner.begin(); 
           iter != scanner.end() && OB_SUCCESS == ret; 
           iter++, index++)
      {
        cell_info.reset();
        ret = iter.get_cell(cell_info);
        if (OB_SUCCESS == ret)
        {
          if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info.value_.get_ext())
          {
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }

          switch (index)
          {
          case 0:
            ret = get_cell_value(cell_info, campaign.campaign_id_);
            break;

          case 1:
            ret = get_cell_value(cell_info, campaign.start_adgroup_id_);
            break;

          case 2:
            ret = get_cell_value(cell_info, campaign.end_adgroup_id_);
            break;

          case 3:
            ret = get_cell_value(cell_info, campaign.start_id_);
            break;

          case 4:
            ret = get_cell_value(cell_info, campaign.end_id_);
            break;

          default:
            TBSYS_LOG(WARN, "more than 5 cells returned for get campaign data");
            ret = OB_ERROR;
            break;
          }
        }
      }

      return ret;
    }

    int ObOlapdriveMeta::store_campaign(
        const int64_t unit_id, 
        const int64_t campaign_id, 
        ObCampaign& campaign)
    {
      int ret = OB_SUCCESS;
      int hash_err = OB_SUCCESS;
      ObCampaignKey campaign_key(unit_id, campaign_id);

      hash_err = campaign_hash_map_.set(campaign_key, campaign);
      if (HASH_INSERT_SUCC == hash_err)
      {
        ret = store_database_campaign(unit_id, campaign_id, campaign);
      }
      else
      {
        TBSYS_LOG(WARN, "failed to set campaign into hash map, hash_err=%d", 
                  hash_err);  
        ret = OB_ERROR; 
      }

      return ret;
    }

    int ObOlapdriveMeta::store_database_campaign(
        const int64_t unit_id, 
        const int64_t campaign_id, 
        ObCampaign& campaign)
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* campaign_schema  = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name_str           = NULL;
      int32_t column_size                   = 0;
      ObString table_name;
      ObObj rowkey_objs[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      ObRowkey row_key;
      ObString column_name;
      ObObj obj;
      ObMutator* mutator = GET_TSI_MULT(ObMutator,TSI_OLAP_MUTATOR_1);

      if (NULL == mutator)
      {
        TBSYS_LOG(ERROR, "failed to get thread local mutator, mutator=%p", mutator);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        mutator->reset();

        //build rowkey
        rowkey_objs[0].set_int(unit_id);
        rowkey_objs[1].set_int(campaign_id);
        row_key.assign (rowkey_objs, 2);
      }

      if (OB_SUCCESS == ret)
      {
        table_name = olapdrive_schema_.get_campaign_name();
  
        //build mutator
        campaign_schema = olapdrive_schema_.get_campaign_schema();
        if (NULL != campaign_schema)
        {
          column_schema = olapdrive_schema_.get_schema_manager().
            get_table_schema(campaign_schema->get_table_id(), column_size);
          for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
          {
            if (NULL != &column_schema[i])
            {
              switch (i)
              {
              case 0:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     campaign.campaign_id_);
                break;

              case 1:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     campaign.start_adgroup_id_);
                break;
  
              case 2:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     campaign.end_adgroup_id_);
                break;
  
              case 3:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     campaign.start_id_);
                break;
  
              case 4:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     campaign.end_id_);
                break;
    
              default:
                TBSYS_LOG(WARN, "more than 5 cells to store for set campaign data");
                ret = OB_ERROR;
                break;
              }
  
              if (OB_SUCCESS == ret)
              {
                column_name_str = column_schema[i].get_name();
                column_name.assign(const_cast<char*>(column_name_str), 
                                   static_cast<int32_t>(strlen(column_name_str)));
                ret = mutator->update(table_name, row_key, column_name, obj);
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
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_.ups_apply(*mutator);
      }

      return ret;
    }

    int ObOlapdriveMeta::get_adgroup(
        const int64_t unit_id, 
        const int64_t campaign_id, 
        const int64_t adgroup_id, 
        ObAdgroup& adgroup)
    {
      int ret = OB_SUCCESS;
      int hash_err = OB_SUCCESS;
      ObAdgroupKey adgroup_key(unit_id, campaign_id, adgroup_id);

      hash_err = adgroup_hash_map_.get(adgroup_key, adgroup);
      if (HASH_NOT_EXIST == hash_err)
      {
        ret = get_database_adgroup(unit_id, campaign_id, adgroup_id, adgroup);
        if (OB_SUCCESS == ret)
        {
          hash_err = adgroup_hash_map_.set(adgroup_key, adgroup);
          if (HASH_INSERT_SUCC != hash_err)
          {
            TBSYS_LOG(WARN, "failed to set adgroup into hash map, hash_err=%d", 
                      hash_err);            
          }
        }
      }
      else if (HASH_EXIST == hash_err)
      {
        //get from hash map, do nothing
      }
      else
      {
        TBSYS_LOG(WARN, "failed to get adgroup from hash map, hash_err=%d", 
                  hash_err);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObOlapdriveMeta::get_database_adgroup(
        const int64_t unit_id, 
        const int64_t campaign_id, 
        const int64_t adgroup_id, 
        ObAdgroup& adgroup)
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* adgroup_schema   = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name               = NULL;
      int32_t column_size                   = 0;
      ObCellInfo cell_info;
      ObString table_name;
      ObObj rowkey_objs[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      ObRowkey row_key;
      ObVersionRange ver_range;

      ObScanner* scanner = GET_TSI_MULT(ObScanner,TSI_OLAP_SCANNER_1);
      ObGetParam* get_param = GET_TSI_MULT(ObGetParam,TSI_OLAP_GET_PARAM_1);

      if (NULL == scanner || NULL == get_param)
      {
        TBSYS_LOG(ERROR, "failed to get thread local get_param or scanner, "
                         "scanner=%p, get_param=%p", scanner, get_param);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        get_param->reset();
        scanner->reset();

        //build rowkey
        rowkey_objs[0].set_int(unit_id);
        rowkey_objs[1].set_int(campaign_id);
        rowkey_objs[2].set_int(adgroup_id);
        row_key.assign (rowkey_objs, 3);
      }

      if (OB_SUCCESS == ret)
      {
        table_name = olapdrive_schema_.get_adgroup_name();
  
        //build get_param
        adgroup_schema = olapdrive_schema_.get_adgroup_schema();
        if (NULL != adgroup_schema)
        {
          column_schema = olapdrive_schema_.get_schema_manager().
            get_table_schema(adgroup_schema->get_table_id(), column_size);
          for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
          {
            cell_info.reset();
            cell_info.table_name_ = table_name;
            cell_info.row_key_ = row_key;
            if (NULL != &column_schema[i])
            {
              column_name = column_schema[i].get_name();
              cell_info.column_name_.assign(const_cast<char*>(column_name), 
                                            static_cast<int32_t>(strlen(column_name)));
              ret = get_param->add_cell(cell_info);
            }
            else
            {
              ret = OB_ERROR;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "adgroup table schema is invalid, adgroup_schema=%p", 
                    adgroup_schema);
          ret = OB_ERROR;
        }
      }
  
      if (OB_SUCCESS == ret)
      {
        get_param->set_is_result_cached(true);
        init_version_range(ver_range);
        get_param->set_version_range(ver_range);
        ret = client_.ms_get(*get_param, *scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to get cell from merge server, rowkey: %s",
              to_cstring(row_key));
        }
      }     

      if (OB_SUCCESS == ret)
      {
        ret = read_adgroup(*scanner, adgroup);
      }
      
      return ret; 
    }

    int ObOlapdriveMeta::read_adgroup(ObScanner& scanner, ObAdgroup& adgroup)
    {
      int ret         = OB_SUCCESS;
      int64_t index   = 0;
      ObScannerIterator iter;
      ObCellInfo cell_info;

      for (iter = scanner.begin(); 
           iter != scanner.end() && OB_SUCCESS == ret; 
           iter++, index++)
      {
        cell_info.reset();
        ret = iter.get_cell(cell_info);
        if (OB_SUCCESS == ret)
        {
          if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info.value_.get_ext())
          {
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }

          switch (index)
          {
          case 0:
            ret = get_cell_value(cell_info, adgroup.campaign_id_);
            break;

          case 1:
            ret = get_cell_value(cell_info, adgroup.adgroup_id_);
            break;

          case 2:
            ret = get_cell_value(cell_info, adgroup.start_id_);
            break;

          case 3:
            ret = get_cell_value(cell_info, adgroup.end_id_);
            break;

          default:
            TBSYS_LOG(WARN, "more than 4 cells returned for get adgroup data");
            ret = OB_ERROR;
            break;
          }
        }
      }

      return ret;
    }

    int ObOlapdriveMeta::store_adgroup(
        const int64_t unit_id, 
        const int64_t campaign_id, 
        const int64_t adgroup_id, 
        ObAdgroup& adgroup)
    {
      int ret = OB_SUCCESS;
      int hash_err = OB_SUCCESS;
      ObAdgroupKey adgroup_key(unit_id, campaign_id, adgroup_id);

      hash_err = adgroup_hash_map_.set(adgroup_key, adgroup);
      if (HASH_INSERT_SUCC == hash_err)
      {
        ret = store_database_adgroup(unit_id, campaign_id, adgroup_id, adgroup);
      }
      else
      {
        TBSYS_LOG(WARN, "failed to set adgroup into hash map, hash_err=%d", 
                  hash_err);  
        ret = OB_ERROR; 
      }

      return ret;
    }

    int ObOlapdriveMeta::store_database_adgroup(
        const int64_t unit_id, 
        const int64_t campaign_id, 
        const int64_t adgroup_id, 
        ObAdgroup& adgroup)
    {
      int ret                               = OB_SUCCESS;
      const ObTableSchema* adgroup_schema   = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name_str           = NULL;
      int32_t column_size                   = 0;
      ObString table_name;
      ObObj rowkey_objs[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      ObRowkey row_key;
      ObString column_name;
      ObObj obj;
      ObMutator* mutator = GET_TSI_MULT(ObMutator,TSI_OLAP_MUTATOR_1);

      if (NULL == mutator)
      {
        TBSYS_LOG(ERROR, "failed to get thread local mutator, mutator=%p", mutator);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        mutator->reset();

        //build rowkey
        rowkey_objs[0].set_int(unit_id);
        rowkey_objs[1].set_int(campaign_id);
        rowkey_objs[2].set_int(adgroup_id);
        row_key.assign (rowkey_objs, 3);
      }

      if (OB_SUCCESS == ret)
      {
        table_name = olapdrive_schema_.get_adgroup_name();
  
        //build mutator
        adgroup_schema = olapdrive_schema_.get_adgroup_schema();
        if (NULL != adgroup_schema)
        {
          column_schema = olapdrive_schema_.get_schema_manager().
            get_table_schema(adgroup_schema->get_table_id(), column_size);
          for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
          {
            if (NULL != &column_schema[i])
            {
              switch (i)
              {
              case 0:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     adgroup.campaign_id_);
                break;

              case 1:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     adgroup.adgroup_id_);
                break;
  
              case 2:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     adgroup.start_id_);
                break;
  
              case 3:
                ret = set_cell_value(obj, column_schema[i].get_type(), 
                                     adgroup.end_id_);
                break;
    
              default:
                TBSYS_LOG(WARN, "more than 4 cells to store for set adgroup data");
                ret = OB_ERROR;
                break;
              }
  
              if (OB_SUCCESS == ret)
              {
                column_name_str = column_schema[i].get_name();
                column_name.assign(const_cast<char*>(column_name_str), 
                                   static_cast<int32_t>(strlen(column_name_str)));
                ret = mutator->update(table_name, row_key, column_name, obj);
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
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_.ups_apply(*mutator);
      }

      return ret;
    }

    void hex_dump_rowkey(const void* data, const int32_t size, 
                         const bool char_type)
    {
      /* dumps size bytes of *data to stdout. Looks like:
       * [0000] 75 6E 6B 6E 6F 77 6E 20
       * 30 FF 00 00 00 00 39 00 unknown 0.....9.
       * (in a single line of course)
       */
    
      unsigned const char *p = (unsigned char*)data;
      unsigned char c = 0;
      int n = 0;
      char bytestr[4] = {0};
      char addrstr[10] = {0};
      char hexstr[ 16*3 + 5] = {0};
      char charstr[16*1 + 5] = {0};
    
      for(n = 1; n <= size; n++) 
      {
        if (n%16 == 1) 
        {
          /* store address for this line */
          snprintf(addrstr, sizeof(addrstr), "%.4x",
              (int)((unsigned long)p-(unsigned long)data) );
        }
    
        c = *p;
        if (isprint(c) == 0) 
        {
          c = '.';
        }
    
        /* store hex str (for left side) */
        snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
        strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);
    
        /* store char str (for right side) */
        snprintf(bytestr, sizeof(bytestr), "%c", c);
        strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);
    
        if (n % 16 == 0)
        { 
          /* line completed */
          if (char_type) 
            fprintf(stderr, "[%4.4s]   %-50.50s  %s\n", 
                addrstr, hexstr, charstr);
          else 
            fprintf(stderr, "[%4.4s]   %-50.50s\n", 
                addrstr, hexstr);
          hexstr[0] = 0;
          charstr[0] = 0;
        } 
        else if(n % 8 == 0) 
        {
          /* half line: add whitespaces */
          strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
          strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
        }
        p++; /* next byte */
      }
    
      if (strlen(hexstr) > 0) 
      {
        /* print rest of buffer if not empty */
        if (char_type) 
          fprintf(stderr, "[%4.4s]   %-50.50s  %s\n", 
              addrstr, hexstr, charstr);
        else 
          fprintf(stderr, "[%4.4s]   %-50.50s\n", 
              addrstr, hexstr);
      }
    }
  } // end namespace olapdrive
} // end namespace oceanbase
