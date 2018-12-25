#include "common/ob_define.h"
#include "task_output.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

TaskOutput::TaskOutput()
{
}

TaskOutput::~TaskOutput()
{
}

int64_t TaskOutput::size(void) const
{
  return file_list_.size();
}

int TaskOutput::add(const uint64_t task_id, const string & peer_ip, const string & file)
{
  int ret = OB_SUCCESS;
  OutputInfo info;
  info.peer_ip_= peer_ip;
  info.file_ = file;
  file_list_.insert(std::pair<uint64_t, OutputInfo>(task_id, info));
  return ret;
}

int TaskOutput::print(FILE * output)
{
  int ret = OB_SUCCESS;
  if (NULL == output)
  {
    ret = OB_ERROR;
  }
  else
  {
    int64_t count = 0;
    map<uint64_t, OutputInfo>::iterator it;
    for (it = file_list_.begin(); it != file_list_.end(); ++it)
    {
      fprintf(output, "%s:%s\n", it->second.peer_ip_.c_str(), it->second.file_.c_str());
      ++count;
    }
  }
  return ret;
}

