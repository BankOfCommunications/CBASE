#ifndef __OB_IMPORT_COMSUMER_H__
#define __OB_IMPORT_COMSUMER_H__

#include "db_queue.h"
#include "file_reader.h"
#include "oceanbase_db.h"
#include "ob_import.h"
#include "ob_import_param.h"
#include "file_appender.h"

class  ImportComsumer : public QueueComsumer<RecordBlock> {
  public:
    ImportComsumer(oceanbase::api::OceanbaseDb *db, ObRowBuilder *builder, AppendableFile *bad_file, const TableParam &param);
    ~ImportComsumer();

    virtual int comsume(RecordBlock &obj);
    virtual int init();
  private:
    int write_bad_record(RecordBlock &rec);

    oceanbase::api::OceanbaseDb *db_;
    ObRowBuilder *builder_;
    const TableParam &param_;
    AppendableFile *bad_file_;
    char *line_buffer_;
    //add by liyongfeng 
    tbsys::CThreadMutex bad_lock_;
    //add:e
};

#endif

