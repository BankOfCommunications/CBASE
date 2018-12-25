#ifndef __OB_IMPORT_PRODUCER_H__
#define  __OB_IMPORT_PRODUCER_H__

#include "db_queue_v2.h"
#include "file_reader_v2.h"

class ImportProducer : public QueueProducer<RecordBlock> {
  public:
    ImportProducer(FileReader &reader, const RecordDelima &delima, const RecordDelima &rec_delima, bool is_rowbyrow, int64_t max_record_count)
      : reader_(reader),
        delima_(delima), rec_delima_(rec_delima),
        is_rowbyrow_(is_rowbyrow), max_record_count_(max_record_count)
    {
      if(is_rowbyrow)
      {
        max_record_count_ = 1;
      }
    }

    int init();

    int produce(RecordBlock &obj);
  private:
    FileReader &reader_;

    RecordDelima delima_;
    RecordDelima rec_delima_;
    bool is_rowbyrow_;
    int64_t max_record_count_;
};

#endif
