#include "ob_black_list.h"

namespace oceanbase
{
  namespace common
  {
//===============================================
//                BlackList
//===============================================
    BlackList::BlackList()
    {
      init();
    }
    BlackList::~BlackList()
    {

    }
    int BlackList::init()
    {
      int ret = OB_SUCCESS;
      if(true)
      {
        server_count_ = 0;
        for(int8_t i = 0; i < OB_INDEX_HANDLE_REP; i++)
        {
          unserved_[i] = 0;
        }
        range_.start_key_.assign(array_, OB_MAX_ROWKEY_COLUMN_NUMBER);
        range_.end_key_.assign(array_ + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
        wok_send_ = 0;
        //init_ = 1;
      }
      return ret;
    }
    //输入的参数是server的指针，使用此函数的时候注意这里应该保证count与数组的长度必须对应
    int BlackList::write_list(ObNewRange &range, ObTabletLocationList &list)
    {
      int ret = OB_SUCCESS;
//      if(NULL == list || count > OB_INDEX_HANDLE_REP)
//      {
//        TBSYS_LOG(ERROR, "invalid argument!");
//        ret = OB_INVALID_ARGUMENT;
//      }
//      else
      {
        range_ = range;
      }
      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0; i < list.size(); i++)
        {
          server_[i] = list[i].server_.chunkserver_;
          unserved_[i] = 0;
        }
        server_count_ = (int8_t)list.size();
      }
      return ret;
    }

    int BlackList::add_in_black_list(ObNewRange &range, ObServer &server)
    {
      int ret = OB_SUCCESS;
      UNUSED(range);
      bool can_add = false;
      //int8_t index =0;
      for(int8_t i = 0; i < server_count_; i++)
      {
        if(server_[i].get_ipv4() == server.get_ipv4() && server_[i].get_port() == server.get_port())
        {
          can_add = true;
          unserved_[i] = 1;
          //index = i;
          break;
        }
      }
      if(!can_add)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "this server is not in black list, we cant turn on server");
      }
      return ret;
    }

    bool BlackList::is_all_repli_failed()
    {
      bool ret = true;
      for(int8_t i = 0; i < server_count_; i++)
      {
        if(unserved_[i] == 0)
        {
          ret = false;
          break;
        }
      }
      return ret;
    }

    int BlackList::next_replic_server(ObServer &server)
    {
      int ret = OB_TABLET_FOR_INDEX_ALL_FAILED;
      for(int8_t i = 0; i < server_count_; i++)
      {
        if(0 == unserved_[i])
        {
          server = server_[i];
          ret = OB_SUCCESS;
          break;
        }
      }
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "all replication is failed......DUMP SERVER:\n"
                  "count[%d],server:[%s][%s][%s],unserved[%d][%d][%d]",server_count_,to_cstring(server_[0]),to_cstring(server_[1]),to_cstring(server_[2])
                  ,unserved_[0],unserved_[1],unserved_[2]);
      }
      return ret;
    }

    void BlackList::get_range(ObNewRange &range)
    {
      range = range_;
    }

    void BlackList::set_range(ObNewRange &range)
    {
      range_ = range;
    }

    //begin from 0
    int BlackList::get_server(int i, ObServer &server)
    {
      int ret = OB_SUCCESS;
      if(i >= (int)server_count_)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        server = server_[i];
      }
      return ret;
    }

    void BlackList::set_server_unserved(ObServer server)
    {
      for(int8_t i =0; i < server_count_; i++)
      {
        if(server.get_ipv4() == server_[i].get_ipv4())
        {
          unserved_[i] = 1;
          break;
        }
      }
    }

    bool BlackList::is_server_unserved(const ObServer &server)
    {
      bool ret = false;
      for(int8_t i =0; i < server_count_; i++)
      {
        if(server.get_ipv4() == server_[i].get_ipv4() && unserved_[i] == 1)
        {
          ret = true;
          break;
        }
      }
      return ret;
    }

#if 0
    int BlackList::get_server(ObServer &server)
    {
      int ret = OB_ERROR;
      for(int8_t i = 0; i < server_count_; i++)
      {
        if(0 == unserved_[i])
        {
          server = server_[i];
          ret = OB_SUCCESS;
          break;
        }
      }
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "all replication is failed......");
      }
      return ret;
    }
#endif
    int8_t BlackList::get_server_count()
    {
      return server_count_;
    }

    DEFINE_SERIALIZE(BlackList)
    {
      int ret = OB_ERROR;
      ObObj obj;
      ret = serialization::encode_i8(buf, buf_len, pos, server_count_);
      if(OB_SUCCESS == ret)
      {
        for(int8_t i = 0; i < server_count_; i++)
        {
          if(OB_SUCCESS != (ret = server_[i].serialize(buf, buf_len, pos)))
          {
            break;
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        for(int8_t i = 0; i < server_count_; i++)
        {
          if(OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, unserved_[i])))
          {
            break;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ret = range_.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    DEFINE_DESERIALIZE(BlackList)
    {
      int ret = OB_ERROR;
      ObObj obj;
      //int64_t table_id = OB_INVALID_ID;
      //int64_t int_value = 0;
      //int8_t border_flag = 0;
      ret = serialization::decode_i8(buf, data_len, pos, &server_count_);

      if(OB_SUCCESS == ret)
      {
        for(int8_t i = 0; i < server_count_; i++)
        {
          ret = server_[i].deserialize(buf, data_len, pos);
          if(OB_SUCCESS != ret)
          {
            break;
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        for(int8_t i = 0; i < server_count_; i++)
        {
          ret = serialization::decode_i8(buf, data_len, pos, unserved_ + i);
          if(OB_SUCCESS != ret)
          {
            break;
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        ret = range_.deserialize(buf, data_len, pos);
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(BlackList)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_i8(server_count_);
      for(int8_t i = 0; i < server_count_; i++)
      {
        total_size += server_[i].get_serialize_size();
        total_size += serialization::encoded_length_i8(unserved_[i]);
      }
      total_size += range_.get_serialize_size();
      return total_size;
    }


//======================================================
//                   BlackListArray
//======================================================
    BlackListArray::BlackListArray()
    {
      init();
    }

    BlackListArray::~BlackListArray()
    {
      pthread_mutex_destroy(&mutex_);
    }

    int BlackListArray::init()
    {
      int ret = OB_SUCCESS;
      pthread_mutex_init(&mutex_, NULL);
      return ret;
    }

    int64_t BlackListArray::get_list_count()
    {
      return list_array_.count();
    }

    BlackList &BlackListArray::get_black_list(int64_t i, int &err)
    {
      //int ret = OB_SUCCESS;
      //BlackList error;
      //int64_t idx = 0;
      if(i > list_array_.count())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"index is greater than black list count");
        return error_;
      }
      else
      {
        return list_array_.at(i);
      }

    }

    int BlackListArray::get_next_black_list(BlackList &list, const ObServer &server)
    {
      int ret = OB_SUCCESS;
      bool has_black_list = false;
      for(int64_t i = 0; i < list_array_.count(); i++)
      {
        if(0 == list_bleach_.at(i) && !(list_array_.at(i).is_server_unserved(server)))
        {
          has_black_list = true;
          list = list_array_.at(i);
          list_bleach_.at(i) = 1;
          break;
        }
      }
      if(!has_black_list)
      {
        ret = OB_ITER_END;
      }
      return ret;
    }

    bool BlackListArray::is_list_array_bleach()
    {
      bool bret = true;
      for(int64_t i = 0; i < list_bleach_.count(); i++)
      {
        if(0 == list_bleach_.at(i))
        {
          bret = false;
          break;
        }
      }
      return bret;
    }

    void BlackListArray::reset()
    {
      list_array_.clear();
      list_bleach_.clear();
      arena_.reuse();
    }
    int BlackListArray::check_range_in_list(ObNewRange &range, bool &in, int64_t &index)
    {
      int ret = OB_SUCCESS;
      in = false;
      ObNewRange list_range;
      for(int64_t i = 0; i < list_array_.count(); i++)
      {
        list_array_.at(i).get_range(list_range);
        if(list_range.table_id_ == range.table_id_ && list_range == range)
        {
          in = true;
          index = i;
          break;
        }
      }
      return ret;
    }
    int BlackListArray::push(BlackList &list)
    {
      int ret = OB_SUCCESS;
      BlackList list_to_push, list_itr;
      ObNewRange range_dst;
      ObNewRange range_src;
      ObNewRange range_itr;
      list.get_range(range_src);
      bool has_new_range = true;
      int64_t index = 0;
      ObServer server;
      for(int64_t i = 0; i < list_array_.count(); i++ )
      {
        if(OB_SUCCESS != (ret = list_array_.at(i, list_itr)))
        {
          TBSYS_LOG(ERROR, "get black list itr failed, ret[%d]", ret);
          break;
        }
        else
        {
          list_itr.get_range(range_itr);
        }
        if(range_itr.table_id_ == range_src.table_id_ && range_itr == range_src)
        {
          has_new_range = false;
          index = i;
          break;
        }
      }
      pthread_mutex_lock(&mutex_);
      if(has_new_range)
      {
        if(OB_SUCCESS != (ret = range_src.start_key_.deep_copy(range_dst.start_key_, arena_)))
        {
          TBSYS_LOG(ERROR, "deep copy start key failed,ret = [%d]", ret);
        }
        else if(OB_SUCCESS != (ret = range_src.end_key_.deep_copy(range_dst.end_key_, arena_)))
        {
          TBSYS_LOG(ERROR, "deep copy end key failed, ret = [%d]", ret);
        }
        else
        {
          range_dst.table_id_ = range_src.table_id_;
          range_dst.border_flag_ = range_src.border_flag_;
          list_to_push.set_range(range_dst);
        }
      }

     if(OB_SUCCESS == ret && has_new_range)
     {
       ObServer server;
       ObTabletLocationList location_list;
       for(int8_t i = 0; i < list.get_server_count(); i++)
       {
         ObTabletLocation location;
         if(OB_SUCCESS != (ret = list.get_server(i, server)))
         {
           TBSYS_LOG(WARN, "get server failed,ret [%d]", ret);
           break;
         }
         else
         {
           location.chunkserver_ = server;
         }
         if(OB_SUCCESS == ret)
         {
           ret = location_list.add(location);
         }
       }
       if(OB_SUCCESS != (ret = list_to_push.write_list(range_dst, location_list)))
       {
         TBSYS_LOG(WARN, "wite list failed,ret = %d", ret);
       }
       else if(OB_SUCCESS != (ret = list_array_.push_back(list_to_push)))
       {
         TBSYS_LOG(WARN, "push list into array failed,ret = %d", ret);
       }
       else
       {
         ret = list_bleach_.push_back(0);
       }
     }

     if(OB_SUCCESS ==  ret && !has_new_range)
     {
      // BlackList &tmp_list = ;
       if(1 == list_bleach_.at(index))
       {
         TBSYS_LOG(INFO, "this black list is already bleach!");
       }
       else
       {
         for(int8_t i = 0; i < list.get_server_count(); i++)
         {
           if(OB_SUCCESS != (ret = list.get_server(i, server)))
           {
             TBSYS_LOG(WARN, "get server failed,ret [%d]", ret);
             break;
           }
           else if(OB_SUCCESS != (ret = list_array_.at(index).add_in_black_list(range_dst, server)))
           {
             TBSYS_LOG(WARN, "add server into black list failed, ret[%d]", ret);
             break;
           }
         }
       }
     }
     pthread_mutex_unlock(&mutex_);

     return ret;
    }

    int BlackList::set_rowkey_obj_array(char *buf, const int64_t buf_len, int64_t &pos, const ObObj *array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == (ret = set_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].serialize(buf, buf_len, pos);
        }
      }
      return ret;
    }

    int BlackList::set_int_obj_value(char *buf, const int64_t buf_len, int64_t &pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int BlackList::get_rowkey_compatible(const char *buf, const int64_t buf_len, int64_t &pos,  ObObj *array, int64_t &size)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      int64_t obj_count = 0;

      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos)) )
      {
        if (ObIntType == obj.get_type() && (OB_SUCCESS == (ret = obj.get_int(obj_count))))
        {
          // new rowkey format.
          for (int64_t i = 0; i < obj_count && OB_SUCCESS == ret; ++i)
          {
            if (i >= size)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else
            {
              ret = array[i].deserialize(buf, buf_len, pos);
            }
          }

          if (OB_SUCCESS == ret) size = obj_count;
        }
        else
        {
          TBSYS_LOG(ERROR, "old fashion rowkey stream!");
          ret = OB_INVALID_ARGUMENT;
        }
      }

      return ret;
    }



  } //end chunkserver
}//end oceanbase
