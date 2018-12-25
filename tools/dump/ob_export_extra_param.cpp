/*
 * =====================================================================================
 *
 *       Filename:  ob_export_extra_param.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月19日 20时55分30秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhangcd 
 *   Organization:  
 *
 * =====================================================================================
 */
#include "ob_export_extra_param.h"

using namespace oceanbase::common;

TabletBlock::~TabletBlock()
{
  if (buffer_ != NULL) {
    free(buffer_);
    buffer_ = NULL;
  }
  
}

int TabletBlock::init()
{
  int ret = OB_SUCCESS;
  buffer_ = (char *)malloc(cap_ * sizeof(char));
  if (NULL == buffer_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "fail to allocate buffer for TabletBlock");
  } else {
    databuff_.set_data(buffer_, cap_);
    inited_ = true;
  }

  return ret;
}

