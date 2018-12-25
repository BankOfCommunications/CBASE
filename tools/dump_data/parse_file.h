#ifndef PARSE_FILE_H_
#define PARSE_FILE_H_


extern int process_file(char * buffer, const int64_t len, FILE * file);

extern int print_scanner(const char * buffer, const int64_t len, int64_t & pos, FILE * file);

#endif //PARSE_FILE_H_
