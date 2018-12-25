#include "ob_check_producer.h"

#include "ob_check.h"
#include "tokenizer.h"
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
	  res = reader_.get_records(obj, rec_delima_, delima_, is_rowbyrow_ ? 1 : INT64_MAX);
	  if(1 == res && reader_.get_buffer_size() > 0) {
		  TBSYS_LOG(WARN, "buffer is not empty, last line has no rec_delima");
	  } else if(res != 0) {
		  TBSYS_LOG(ERROR, "can't get record");
		  ret = ComsumerQueue<RecordBlock>::QUEUE_ERROR;
	  }
  } else if ( reader_.get_buffer_size() > 0 ) {
	error_arr[3] = false; //add by pangtz:20141205
    TBSYS_LOG(ERROR, "buffer is not empty, maybe last line has no rec_delima, please check");
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  } else {
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  }

  return ret;
}
