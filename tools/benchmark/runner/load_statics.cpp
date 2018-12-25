#include "load_statics.h"

using namespace oceanbase::tools;

Statics Statics::operator - (const Statics & other) const
{
  Statics inc;
  // share info
  inc.share_info_.produce_count_ = share_info_.produce_count_ - other.share_info_.produce_count_;
  inc.share_info_.filter_count_ = share_info_.filter_count_ - other.share_info_.filter_count_;
  inc.compare_err_ = compare_err_ - other.compare_err_;
  // new server
  inc.new_server_.consume_count_ = new_server_.consume_count_ - other.new_server_.consume_count_;
  inc.new_server_.succ_time_used_ = new_server_.succ_time_used_ - other.new_server_.succ_time_used_;
  inc.new_server_.succ_count_ = new_server_.succ_count_ - other.new_server_.succ_count_;
  inc.new_server_.err_count_ = new_server_.err_count_ - other.new_server_.err_count_;
  inc.new_server_.err_time_used_ = new_server_.err_time_used_ - other.new_server_.err_time_used_;
  inc.new_server_.return_res_ = new_server_.return_res_ - other.new_server_.return_res_;
  // old server
  inc.old_server_.consume_count_ = old_server_.consume_count_ - other.old_server_.consume_count_;
  inc.old_server_.succ_time_used_ = old_server_.succ_time_used_ - other.old_server_.succ_time_used_;
  inc.old_server_.succ_count_ = old_server_.succ_count_ - other.old_server_.succ_count_;
  inc.old_server_.err_count_ = old_server_.err_count_ - other.old_server_.err_count_;
  inc.old_server_.err_time_used_ = old_server_.err_time_used_ - other.old_server_.err_time_used_;
  inc.old_server_.return_res_ = old_server_.return_res_ - other.old_server_.return_res_;
  return inc;
}

void Statics::print(const bool old, const char * name, FILE * file) const
{
  if (file != NULL)
  {
    print(share_info_, name, new_server_, file);
    if (true == old)
    {
      print(share_info_, name, old_server_, file);
    }
    fflush(file);
  }
}

void Statics::print(const ProduceInfo & producer, const char * section,
    const ConsumeInfo & consumer, FILE * file) const
{
  UNUSED(producer);
  UNUSED(section);
  UNUSED(consumer);
  UNUSED(file);
  //char buffer[256] = "";
  //time_t t = time(NULL);
  //strftime(buffer, sizeof(buffer), "%H:%M:%S", localtime(&t));
  //fprintf(file, "%9s %9s %9s %9s %9s %9s %9s %9s %9s %9s %9s %9s %9s\n", buffer, section,
  //    "FILTER", "PRODUCE", "CONSUME", "TOTIME", "AVGRES", "SUCCNT", "SUTIME", "RATIO", "ERROR", "ERRTIME", "COMPARE");
  //int64_t total_read_count = producer.filter_count_ + producer.produce_count_;
  //int64_t total_consume_count = consumer.succ_count_ + consumer.err_count_;
  //fprintf(file, "%9s %9ld %9ld %9ld %9ld %9.2f %9.2f %9ld %9.2f %9.2f %9ld %9.2f %9ld\n", buffer,
  //    total_read_count, producer.filter_count_, producer.produce_count_, consumer.consume_count_,
  //    (total_consume_count == 0) ? 0.0 : (consumer.succ_time_used_ + consumer.err_time_used_) / (double)total_consume_count,
  //    (total_consume_count == 0) ? 0 : consumer.return_res_ / (double)total_consume_count,
  //    consumer.succ_count_, (consumer.succ_count_ == 0) ? 0.0 : consumer.succ_time_used_ / (double)consumer.succ_count_,
  //    (total_consume_count == 0) ? 0.0 : (consumer.succ_count_ / (double)total_consume_count) * 100,
  //    consumer.err_count_, (consumer.err_count_ == 0) ? 0.0 : (consumer.err_time_used_/ (double)consumer.err_count_),
  //    compare_err_);
}


