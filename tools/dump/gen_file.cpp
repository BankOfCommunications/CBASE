#include <string>
#include <vector>
#include "common/file_utils.h"

using namespace std;
using namespace oceanbase::common;

int64_t linenr  = 0;
int64_t single = 0;

string &gen_str()
{
  static string str;
  str.clear();
  char *dict = "abcedfghijklmnopqrstuvwxyz";
  char *single_dict = "abcedfghijklmnopqrstuvwxyz";

  int len = rand() % 50;
  char last_char = 0;
  char curr_char = 0;

  for(int i = 0;i < len; i++) {
    while (1) {
      if (single) {
        int idx = rand() % strlen(single_dict);
        curr_char = single_dict[idx];
      } else {
        int idx = rand() % strlen(dict);
        curr_char = dict[idx];
      }

      if (curr_char == '\t' && last_char == 1) {
        continue;
      }

      if (curr_char == '\n' && last_char == 2) {
        continue;
      }

      break;
    }

    str += curr_char;
    last_char = curr_char;
  }

  return str;
}

vector<string> no_delima;

void gen_file(vector<string> &v)
{
  char buf[4096];
  char no_delima_buf[4096];
  char *date = "2011-08-09 03:10:12";
  //int idate = rand();

  for(int i = 0;i < linenr; i++) {
//    snprintf(buf, 4096, "%d\01%d\01%d\01%f\01%d\01%d\01%d\01%d\01%d\n", 
//             i, i, idate, (float)i, 123, i, idate, idate, idate);
    string str = gen_str();
    string str1 = gen_str();

    if (single) {
      snprintf(buf, 4096, "%d\01%s\01%s\01%s\n", i, str.c_str(), str1.c_str(), date);
    } else {
      snprintf(buf, 4096, "%d\01\t%s\01\t%s\01\t%s\02\n", i, str.c_str(), str1.c_str(), date);
    }

    snprintf(no_delima_buf, 4096, "%d\t%s\t%s\n", i, str.c_str(), str1.c_str());

    v.push_back(buf);
    no_delima.push_back(no_delima_buf);
  }
}

void output_file(vector<string> &v, const char *file_name)
{
  FileUtils file;
  file.open(file_name, O_WRONLY | O_TRUNC | O_CREAT, 0644);

  for (size_t i = 0;i < v.size();i++) {
    file.write(v[i].c_str(), v[i].size());
  }
  file.close();
}

void gen_data_file()
{
  vector<string> v;
  gen_file(v);
  output_file(v, "test1.txt");
  output_file(no_delima, "test1_no.txt");
}

int main(int argc, char *argv[])
{
  if (argc != 3) {
    fprintf(stderr, "gen_file [line no] single\n");
    exit(0);
  }

  linenr = atol(argv[1]);
  single = atol(argv[2]);
  gen_data_file();
}
