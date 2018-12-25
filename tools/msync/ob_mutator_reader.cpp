/** (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#include "Time.h"
#include "ob_mutator_reader.h"
#include "common/ob_schema.h"

using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace msync
  {
    static bool is_normal_ups_mutator(ObUpsMutator& ups_mutator)
    {
      return ups_mutator.is_normal_mutator();
    }
    
    // static void dump_ob_mutator_cell(ObMutatorCellInfo& cell)
    // {
    //   uint64_t op = cell.op_type.get_ext();
    //   uint64_t table_id = cell.cell_info.table_id_;
    //   uint64_t column_id = cell.cell_info.column_id_;
    //   TBSYS_LOG(INFO, "cell{op=%lu, table_id=%lu, column_id=%lu", op, table_id, column_id);
    // }
    
    int cell_info_resolve_table_name(ObSchemaManagerV2& sch_mgr, ObCellInfo& cell)
    {
      int err = OB_SUCCESS;
      uint64_t table_id = cell.table_id_;
      const ObTableSchema* table_schema = NULL;
      const char* table_name = NULL;
      // `table_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
      if (OB_INVALID_ID != table_id)
      {
        if (NULL == (table_schema = sch_mgr.get_table_schema(table_id)))
        {
          err = OB_SCHEMA_ERROR;
          TBSYS_LOG(WARN, "sch_mge.get_table_schema(table_id=%lu)=>NULL", table_id);
        }
        else if (NULL == (table_name = table_schema->get_table_name()))
        {
          err = OB_SCHEMA_ERROR;
          TBSYS_LOG(ERROR, "get_table_name(table_id=%lu) == NULL", table_id);
        }
        else
        {
          cell.table_name_.assign_ptr((char*)table_name, static_cast<int32_t>(strlen(table_name)));
          //cell.table_id_ = OB_INVALID_ID;
        }
      }
      return err;
    }

    int cell_info_resolve_column_name(ObSchemaManagerV2& sch_mgr, ObCellInfo& cell)
    {
      int err = OB_SUCCESS;
      uint64_t table_id = cell.table_id_;
      uint64_t column_id = cell.column_id_;
      const ObColumnSchemaV2* column_schema = NULL;
      const char* column_name = NULL;
      // `table_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
      // `column_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
      //                                                        or cell.op_type == OB_DEL_ROW
      if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_id)
      {
        if (NULL == (column_schema = sch_mgr.get_column_schema(table_id, column_id)))
        {
          err = OB_SCHEMA_ERROR;
          TBSYS_LOG(ERROR, "sch_mgr.get_column_schema(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
        }
        else if(NULL == (column_name = column_schema->get_name()))
        {
          err = OB_SCHEMA_ERROR;
          TBSYS_LOG(ERROR, "get_column_name(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
        }
        else
        {
          cell.column_name_.assign_ptr((char*)column_name, static_cast<int32_t>(strlen(column_name)));
          //cell.column_id_ = OB_INVALID_ID;
        }
      }
      return err;
    }

    int ob_mutator_resolve_name(ObSchemaManagerV2& sch_mgr, ObMutator& mut)
    {
      int err = OB_SUCCESS;
      while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
      {
        ObMutatorCellInfo* cell = NULL;
        if (OB_SUCCESS != (err = mut.get_cell(&cell)))
        {
          TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
        }
        else if (OB_SUCCESS != (err = cell_info_resolve_column_name(sch_mgr, cell->cell_info)))
        {
          TBSYS_LOG(ERROR, "resolve_column_name(table_id=%lu, column_id=%lu)=>%d",
                    cell->cell_info.table_id_, cell->cell_info.column_id_, err);
        }
        else if (OB_SUCCESS != (err = cell_info_resolve_table_name(sch_mgr, cell->cell_info)))
        {
          TBSYS_LOG(ERROR, "resolve_table_name(table_id=%lu)=>%d", cell->cell_info.table_id_, err);
        }
      }
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      return err;
    }

    int ObMutatorReader::initialize(const char* schema, const char* log_dir, const uint64_t log_file_start_id,
                                    const uint64_t last_seq_id, int64_t wait_time)
    {
      int err = OB_SUCCESS;
      tbsys::CConfig config;
      wait_time_ = wait_time;
      if(OB_SUCCESS != (err = (sch_mgr_.parse_from_file(schema, config)? OB_SUCCESS: OB_INVALID_ARGUMENT)))
      {
        TBSYS_LOG(WARN, "schemaNameMap.initialize(schema='%s')=>%d", schema, err);
      }
      else if (OB_SUCCESS != (err = reader_.init(&repeated_reader_, log_dir, log_file_start_id, last_seq_id, true)))
      {
        TBSYS_LOG(WARN, "reader.initialize(log_dir='%s', log_file_id=%lu, last_seq_id=%lu)=>%d",
                  log_dir, log_file_start_id, last_seq_id, err);
      }
      return err;
    }

    int ObMutatorReader::read(ObDataBuffer& buffer, uint64_t& seq)
    {
      int err = OB_SUCCESS;
      buffer.set_data(mutator_buffer_, sizeof(mutator_buffer_));
      buffer.get_position() = 0;
      uint64_t tmp_seq = 0;
      if (OB_SUCCESS != (err = accumulated_mutator_.reset()))
      {
        TBSYS_LOG(ERROR, "ObMutator.reset()=>%d", err);
      }
      else if (OB_SUCCESS != (err = read_(accumulated_mutator_, tmp_seq, buffer.get_capacity())))
      {
        TBSYS_LOG(ERROR, "ObMutatorReader.read(limit=%ld)=>%d", buffer.get_capacity(), err);
      }
      else if (OB_SUCCESS != (err = accumulated_mutator_.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "ObMutator.serialize(buf=%p, len=%ld, pos=%ld)=>%d",
                  buffer.get_data(), buffer.get_capacity(), 0L, err);
      }
      else
      {
        seq = tmp_seq;
      }
      return err;
    }

    int mutator_add_(ObMutator& dst, ObMutator& src)
    {
      int err = OB_SUCCESS;
      src.reset_iter();
      while ((OB_SUCCESS == err) && (OB_SUCCESS == (err = src.next_cell())))
      {
        ObMutatorCellInfo* cell = NULL;
        if (OB_SUCCESS != (err = src.get_cell(&cell)))
        {
          TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
        }
        else if (OB_SUCCESS != (err = dst.add_cell(*cell)))
        {
          TBSYS_LOG(ERROR, "dst.add_cell()=>%d", err);
        }
      }
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      return err;
    }
    
    int mutator_add(ObMutator& dst, ObMutator& src, int64_t size_limit)
    {
      int err = OB_SUCCESS;
      if (dst.get_serialize_size() + src.get_serialize_size() > size_limit)
      {
        err = OB_SIZE_OVERFLOW;
        TBSYS_LOG(DEBUG, "mutator_add(): size overflow");
      }
      else if (OB_SUCCESS != (err = mutator_add_(dst, src)))
      {
        TBSYS_LOG(ERROR, "mutator_add()=>%d", err);
      }
      return err;
    }

    int ObMutatorReader::read_(ObMutator& mutator, uint64_t& seq, int64_t size_limit)
    {
      int err = OB_SUCCESS;
      ObMutator* cur_mutator = NULL;
      uint64_t tmp_seq = 0;
      int64_t timeout = 1LL<<60;
      bool read_nothing = true;
      int64_t last_mutator_size = mutator.get_serialize_size();
      int64_t dec_size = 0;
      while(OB_SUCCESS  == err)
      {
        //TBSYS_LOG(DEBUG, "mutator.get_serialize_size()=%ld", mutator.get_serialize_size());
        if (OB_SUCCESS != (err = get_mutator(cur_mutator, tmp_seq, timeout)))
        {
          if(OB_READ_NOTHING != err)
          {
            TBSYS_LOG(ERROR, "ObMutatorReader.get_mutator(last_seq=%ld)=>%d", last_seq_, err);
          }
          else
          {
            TBSYS_LOG(DEBUG, "ObMutatorReader.get_mutator(last_seq=%ld)=>%d", last_seq_, err);
          }
        }
        else if (OB_SUCCESS != (err = mutator_add(mutator, *cur_mutator, size_limit)))
        {
          if(OB_SIZE_OVERFLOW != err)
          {
            TBSYS_LOG(ERROR, "ObMutatorReader::mutator_add(last_seq=%ld)=>%d", last_seq_, err);
          }
          else
          {
            TBSYS_LOG(DEBUG, "ObMutatorReader::mutator_add(last_seq=%ld)=>%d", last_seq_, err);
          }
        }
        else
        {
          read_nothing = false;
          seq = tmp_seq;
          consume_mutator();
          dec_size = last_mutator_size + cur_mutator->get_serialize_size() - mutator.get_serialize_size();
          last_mutator_size = mutator.get_serialize_size();
          if (dec_size <= 0)
          {
            TBSYS_LOG(ERROR, "size inc after mutator merge: %ld", -dec_size);
            err = OB_ERR_UNEXPECTED;
          }
        }
        timeout = 0;
      }
      if (!read_nothing)
      {
        err = OB_SUCCESS;
      }
      return err;
    }

    int ObMutatorReader::consume_mutator()
    {
      last_mutator_ = NULL;
      return OB_SUCCESS;
    }
    
    int ObMutatorReader::get_mutator(ObMutator*& mutator, uint64_t& seq, int64_t timeout)
    {
      int err = OB_SUCCESS;
      uint64_t tmp_seq = 0;
      if (NULL != last_mutator_)
      {} // cached
      else if (OB_SUCCESS != (err = get_mutator_(mutator, tmp_seq, timeout)))
      {
        if (OB_READ_NOTHING != err)
        {
          TBSYS_LOG(ERROR, "get_mutator_(last_seq=%ld)=>%d", last_seq_, err);
        }
      }
      else
      {
        last_seq_ = tmp_seq;
        last_mutator_ = mutator;
      }
      if (OB_SUCCESS == err)
      {
        seq = last_seq_;
        mutator = last_mutator_;
      }
      return err;
    }
    
    int ObMutatorReader::get_mutator_(ObMutator*& mutator, uint64_t& seq, int64_t timeout)
    {
      int err = OB_SUCCESS;
      ObMutator* tmp_mutator = NULL;
      if (OB_SUCCESS != (err = read_single_mutator_(tmp_mutator, seq, timeout)))
      {
        if(OB_READ_NOTHING != err)
        {
          TBSYS_LOG(ERROR, "ObMutatorReader.read()=>%d", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "ObMutatorReader.read()=>%d", err);
        }
      }
      else if (OB_SUCCESS != (err =  ob_mutator_resolve_name(sch_mgr_, *tmp_mutator)))
      { // after `ob_mutator_resolve_name()', `tmp_mutator.get_serialize_size()'
        // return wrong serialize size.
        TBSYS_LOG(ERROR, "ups_mutator_resolve_name()=>%d", err);
      }
      else if (OB_SUCCESS != (err = cur_mutator_.reset()))
      {
        TBSYS_LOG(ERROR, "cur_mutator_.reset()=>%d", err);
      }
      else if (OB_SUCCESS != (err = mutator_add(cur_mutator_, *tmp_mutator, INT64_MAX)))
      { // copy ObMutator to make `cur_mutator_.get_serialize_size()' return right serialize size.
        TBSYS_LOG(ERROR, "mutator_add()=>%d", err);
      }
      else
      {
        mutator = &cur_mutator_;
      }
      return err;
    }

    int ObMutatorReader::read_single_mutator_(ObMutator*& mutator, uint64_t& seq, int64_t timeout)
    {
      int err = OB_SUCCESS;
      tbutil::Time now = tbutil::Time::now();
      LogCommand cmd = OB_LOG_UNKNOWN;
      int64_t pos = 0;
      char* buf = NULL;
      int64_t data_len = 0;
      while(!stop_)
      {
        err = reader_.read_log(cmd, seq, buf, data_len);
        if (OB_SUCCESS == err && OB_LOG_UPS_MUTATOR == cmd)
        {
          pos = 0;
          if (OB_SUCCESS  != (err = cur_ups_mutator_.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(ERROR, "ObUpsMutator.deserialize(buf=%p, len=%ld, pos=%ld)=>%d",
                      buf, data_len, pos, err);
            break;
          }
          else if (is_normal_ups_mutator(cur_ups_mutator_))
          {
            mutator = &cur_ups_mutator_.get_mutator();
            break;
          }
        }
        if (OB_SUCCESS != err && OB_READ_NOTHING != err)
        {
          TBSYS_LOG(ERROR, "reader.read(seq=%ld)=>%d", seq, err);
          break; // error cause exit
        }
        
        if (OB_READ_NOTHING == err && (tbutil::Time::now() - now).toMilliSeconds() > timeout/1000)
        {
          break; // timeout cause exit
        }
        if (OB_READ_NOTHING == err)
        {
          usleep(static_cast<useconds_t>(wait_time_));
        }
      }
      return err;
    }
  } // end namespace msync
} // end namespace oceanbase
