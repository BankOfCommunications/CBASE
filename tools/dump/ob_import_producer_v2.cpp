#include "ob_import_producer_v2.h"

#include "ob_import_v2.h"
#include "tokenizer_v2.h"
#include "slice.h"

using namespace oceanbase::common;

int ImportProducer::init()
{
  return OB_SUCCESS;
}

int ImportProducer::produce(RecordBlock &obj)
{
  int ret = ComsumerQueue<RecordBlock>::QUEUE_SUCCESS;
  int res = 0;

  if (!reader_.eof()) {
    res = reader_.get_records(obj, rec_delima_, delima_, max_record_count_);
    if(res == OB_ARRAY_OUT_OF_RANGE)//add by zhuxh:20151201
    {
      TBSYS_LOG(ERROR, "Transformation from GBK to UTF-8 error");
      error_arr[G2U_ERROR]=false;
      error_arr[BUFFER_OVER_FLOW]=false;
      ret = ComsumerQueue<RecordBlock>::QUEUE_ERROR;
    }
    else if(1 == res && reader_.get_buffer_size() > 0)
    {
      //mod by zhuxh:20151130 :b
      TBSYS_LOG(ERROR, "buffer is not empty, last line has no rec_delima (May be caused by odd number of \")");
      error_arr[INCOMPLETE_DATA]=false;
      ret = ComsumerQueue<RecordBlock>::QUEUE_ERROR;
      //mod :e
    } else if(res != 0) {
      TBSYS_LOG(ERROR, "can't get record");
      error_arr[G2U_ERROR]=false; //add by zhuxh: 20161208 [bug of code conversion error without declaration]
      ret = ComsumerQueue<RecordBlock>::QUEUE_ERROR;
    }
  } else if ( reader_.get_buffer_size() > 0 ) {
    error_arr[DATA_ERROR] = false; //add by pangtz:20141205 //modify by zhuxh:20150720
    TBSYS_LOG(ERROR, "buffer is not empty, maybe last line has no rec_delima, please check");
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  } else {
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  }

  return ret;
}
