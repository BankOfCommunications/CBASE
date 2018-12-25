#ifndef __OB_CHECK_PRODUCER_H__
#define  __OB_CHECK_PRODUCER_H__

#include "db_queue_check.h"
#include "file_reader.h"

class ImportProducer : public QueueProducer<RecordBlock> {
  public:
    ImportProducer(FileReader &reader, const RecordDelima &delima, const RecordDelima &rec_delima, bool is_rowbyrow)
      : reader_(reader), delima_(delima), rec_delima_(rec_delima), is_rowbyrow_(is_rowbyrow) { }

    int init();

    int produce(RecordBlock &obj);
  private:
    FileReader &reader_;

    RecordDelima delima_;
    RecordDelima rec_delima_;
    bool is_rowbyrow_;
};

#endif
