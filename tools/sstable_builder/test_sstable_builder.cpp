#include <assert.h>
#include <stdio.h>
#include "sstable_builder.h"

int main(int argc, char **argv)
{
  int ret = 0;
  char* buffer = NULL;
  int pos = 0;
  const char* output = NULL;
  long output_size = 0;
  FILE *fp = NULL;

  (void)argc;
  (void)argv;
  
  ret = init("schema.ini", "data_syntax.ini", 1007, true);
  assert(ret == 0);

  buffer = (char*)malloc(2 * 1024 *1024);
  assert(NULL != buffer);

  fp = fopen("123456", "w+");
  assert(NULL != fp);

  for (int i = 0; i < 2500; i++)
  {
    pos += sprintf(buffer + pos, "%s %04d %s %d %d %d\n", 
      "Key:", i, "testtesttest", i, i, i);
  }
  ret = append(buffer, pos, true, false, true, true, &output, &output_size);
  assert(ret == 0);
  ret = static_cast<int32_t>(fwrite(output, 1, output_size, fp));
  assert(ret == output_size);

  pos = 0;
  for (int i = 2500; i < 5000; i++)
  {
    pos += sprintf(buffer + pos, "%s %04d %s %d %d %d\n",
      "Key:", i, "testtesttest", i, i, i);
  }
  ret = append(buffer, pos, false, false, false, false, &output, &output_size);
  assert(ret == 0);
  ret = static_cast<int32_t>(fwrite(output, 1, output_size, fp));
  assert(ret == output_size);

  pos = 0;
  for (int i = 5000; i < 7500; i++)
  {
    pos += sprintf(buffer + pos, "%s %04d %s %d %d %d\n",
      "Key:", i, "testtesttest", i, i, i);
  }
  ret = append(buffer, pos, false, false, false, false, &output, &output_size);
  assert(ret == 0);
  ret = static_cast<int32_t>(fwrite(output, 1, output_size, fp));
  assert(ret == output_size);

  pos = 0;
  for (int i = 7500; i < 10000; i++)
  {
    pos += sprintf(buffer + pos, "%s %04d %s %d %d %d\n",
      "Key:", i, "testtesttest", i, i, i);
  }
  ret = append(buffer, pos, false, true, false, false, &output, &output_size);
  assert(ret == 0);
  ret = static_cast<int32_t>(fwrite(output, 1, output_size, fp));
  assert(ret == output_size);

//pos = 0;
//ret = append(buffer, pos, false, true, &output, &output_size);
//assert(ret == 0);
//assert(NULL != output);
//assert(output_size > 0);
//ret = fwrite(output, 1, output_size, fp);
//assert(ret == output_size);

  ret = fclose(fp);
  assert(ret == 0);

  return 0;
}
