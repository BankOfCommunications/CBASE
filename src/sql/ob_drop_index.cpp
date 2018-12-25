/*
 *add wenghaixing [secondary index drop index]201412222
 *
 *This source file is used to drop index
 */
#include "sql/ob_drop_index.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDropIndex::ObDropIndex()
    :rpc_(NULL)
{

}

ObDropIndex::~ObDropIndex()
{

}

void ObDropIndex::reset()
{
  rpc_=NULL;
}

void ObDropIndex::reuse()
{
  rpc_=NULL;
}

int ObDropIndex::add_index_name(const common::ObString &tname)
{
  return indexs_.add_string(tname);
}

int ObDropIndex::open()
{
  int ret = OB_SUCCESS;
  //modify liuxiao[secondary index bug fix]
  //if(NULL == rpc_||0 >= indexs_.count())
  //{
  //  ret=OB_NOT_INIT;
  //  TBSYS_LOG(ERROR,"not init,prc_=%p",rpc_);
  //}
  //在没有索引表可以删除情况下返回信息
  if(NULL == rpc_)
  {
    ret=OB_NOT_INIT;
    TBSYS_LOG(ERROR,"not init,prc_=%p",rpc_);
  }
  else if(0 >= indexs_.count())
  {
    ret=OB_INDEX_NOT_EXIST;
    TBSYS_LOG(WARN,"not index to drop");
  }
  //modify e
  else if(OB_SUCCESS != (ret = rpc_->drop_index(indexs_)))
  {
    TBSYS_LOG(WARN,"failed to drop index,err=%d",ret);
  }
  else
  {
    TBSYS_LOG(INFO,"drop index succ,tables=[%s]",
                  to_cstring(indexs_));
  }
  return ret;
}

int ObDropIndex::close()
{
  int ret=OB_SUCCESS;
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObDropIndex, PHY_DROP_INDEX);
  }
}

int64_t ObDropIndex::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "DropIndexs");
  pos += indexs_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}


