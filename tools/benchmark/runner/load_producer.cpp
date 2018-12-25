#include "load_manager.h"
#include "load_util.h"
#include "load_filter.h"
#include "load_producer.h"
#include "common/ob_get_param.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

LoadProducer::LoadProducer()
{
  _stop = false;
  filter_ = NULL;
  manager_ = NULL;
}

LoadProducer::LoadProducer(const SourceParam & param, LoadFilter * filter, LoadManager * manager)
{
  _stop = false;
  param_ = param;
  filter_ = filter;
  manager_ = manager;
}

LoadProducer::~LoadProducer()
{
}

void LoadProducer::init(const SourceParam & param, LoadFilter * filter, LoadManager * manager, const bool read_only)
{
  param_ = param;
  filter_ = filter;
  manager_ = manager;
  read_only_ = read_only;
}

void LoadProducer::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(arg);
  if (manager_ != NULL)
  {
    int64_t count = 0;
    for (int64_t i = 0; (i < param_.iter_times_) && !_stop; ++i)
    {
      ret = load_file(param_.file_, param_.stream_server_, count);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "load input load file failed:ret[%d]", ret);
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "load one file request succ:count[%ld]", count);
        //break;
      }
    }
    manager_->set_finish();
  }
  else
  {
    TBSYS_LOG(WARN, "check load manager failed:load[%p]", manager_);
  }
}

int LoadProducer::load_file(const char * file, const ObServer & server, int64_t & count)
{
  int ret = OB_SUCCESS;
  UNUSED(server);
  if (NULL == file)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check file failed:file[%s]", file);
  }
  else
  {
    QueryInfo query;
    FIFOPacket header;
    uint32_t data_len = 0;
    static __thread char buffer[OB_MAX_PACKET_LENGTH];
    FILE * pfile = fopen(file, "r");
    count = 0;
    int64_t last_time = tbsys::CTimeUtil::getTime();
    int64_t cur_time = 0;
    int64_t tmp_count = 0;
    if (pfile != NULL)
    {
      while (!_stop)
      {
        ret = read_request(pfile, header, buffer, (uint32_t)OB_MAX_PACKET_LENGTH, data_len);
        if (FILE_END == ret)
        {
          ret = OB_SUCCESS;
          break;
        }
        else if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "read packet failed:ret[%d]", ret);
          break;
        }
        ret = construct_request(header, buffer, data_len, query);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "construct task failed:ret[%d]", ret);
          break;
        }
        // push to the task queue
        ret = manager_->push(query, read_only_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "push new task failed:ret[%d]", ret);
        }
        else
        {
          ++count;
          cur_time = tbsys::CTimeUtil::getTime();
        }
        if (cur_time - last_time > 10 * 1000 * 1000)
        {
          TBSYS_LOG(INFO, "producer: read count = %ld per 10Second", count - tmp_count);
          manager_->dump();
          last_time = cur_time;
          tmp_count = count;
        }
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "fopen failed:file[%s], ret[%d]", file, ret);
    }
    if (pfile != NULL)
    {
      fclose(pfile);
      pfile = NULL;
    }
  }
  return ret;
}

int LoadProducer::read_request(FILE * file, FIFOPacket & header,
    char buffer[], const uint32_t max_len, uint32_t & data_len)
{
  int ret = OB_SUCCESS;
  int32_t len = 0;
  int32_t size = sizeof(FIFOPacket);
  len = int32_t(fread(&header, size, int32_t(1), file));
  if (len <= 0)
  {
    if (true == feof(file))
    {
      TBSYS_LOG(INFO, "read file end");
      ret = FILE_END;
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "fread header size failed:size[%u]", len);
    }
  }
  else if ((data_len = header.buf_size) > max_len)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check data len too long:len[%u], max[%u]", data_len, max_len);
  }
  else
  {
    // not support reading appending file
    if (fread(buffer, data_len, 1, file) <= 0)
    {
      TBSYS_LOG(WARN, "fread data size failed:len[%u]", data_len);
    }
  }
  return ret;
}

int LoadProducer::construct_request(const FIFOPacket & header, const char buffer[],
    const int64_t data_len, QueryInfo & query)
{
  int ret = OB_SUCCESS;
  static int64_t id = 0;
  query.id_ = id++;
  query.packet_ = reinterpret_cast<FIFOPacket *> (malloc(data_len + sizeof(FIFOPacket)));
  if (NULL != query.packet_)
  {
    memcpy(query.packet_, &header, sizeof(FIFOPacket));
    memcpy(((char*)query.packet_ + sizeof(FIFOPacket)), buffer, data_len);
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "malloc memory for packet failed:buf[%p], len[%ld]", buffer, data_len);
  }
  return ret;
}

void LoadProducer::destroy_request(QueryInfo & query)
{
  if (query.packet_ != NULL)
  {
    free(query.packet_);
    query.packet_ = NULL;
  }
}

