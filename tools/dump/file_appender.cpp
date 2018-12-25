#include "file_appender.h"

const int kPageSize = 8 * 1024 * 1024; //8M

int AppendableFile::NewAppendableFile(const std::string &name, AppendableFile *&result)
{
  return NewAppendableFile(name.c_str(), result);
}

int AppendableFile::NewAppendableFile(const char *name, AppendableFile *&result)
{
  int ret = OB_SUCCESS;
  result = NULL;
  const int fd = open(name, O_CREAT | O_RDWR | O_TRUNC, 0644);
  if (fd < 0) {
    TBSYS_LOG(ERROR, "error when open %s, due to %s", name, strerror(errno));
    ret = OB_ERROR;
  } else {
    result = new(std::nothrow) AppendableFile(name, fd, kPageSize);

    if (result == NULL) {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "can't allocate AppendableFile");
    }
  }

  return ret;
}
