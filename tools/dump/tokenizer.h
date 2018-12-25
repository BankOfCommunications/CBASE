#ifndef __TOKENIZER_H__
#define  __TOKENIZER_H__
#include <string>
#include <vector>
#include <stdint.h>
#include "slice.h"

struct TokenInfo {
  TokenInfo() : token(NULL), len(0) { }

  const char *token;
  int64_t len;
};

//add by pangtz:20141126 声明TableParam
struct TableParam;
//add:end
extern bool trim;

class RecordDelima {
  public:
    enum RecordDelimaType {
      CHAR_DELIMA,
      SHORT_DELIMA
    };
  public:
    RecordDelima() {
      type_ = CHAR_DELIMA;
      part1_ = 1;
    }

    RecordDelima(char delima) {
      part1_ = delima;
      type_ = CHAR_DELIMA;
    }

    RecordDelima(char part1, char part2) {
      part1_ = part1;
      part2_ = part2;
      type_ = SHORT_DELIMA;
    }

    void set_short_delima(char part1, char part2) {
      part1_ = part1;
      part2_ = part2;
      type_ = SHORT_DELIMA;
    }

    void set_char_delima(char delima) {
      part1_ = delima;
      type_ = CHAR_DELIMA;
    }

    unsigned char get_char_delima() const {
      return part1_;
    }

    bool is_delima(const char *buff, int64_t pos, int64_t buff_size) const;

    RecordDelimaType delima_type() const { return type_; }

    void skip_delima(int64_t &pos) const { pos += type_ == CHAR_DELIMA ? 1 : 2; }

    void append_delima(char *buff, int64_t pos, int64_t buff_size) const {
      if (type_ == CHAR_DELIMA && pos < buff_size) {
        buff[pos] = part1_;
      } else if (type_ == SHORT_DELIMA && (pos + 1) < buff_size) {
        buff[pos] = part1_;
        buff[pos + 1] = part2_;
      }
    }

    int length() const { return type_ == CHAR_DELIMA ? 1 : 2; }

  public:
    unsigned char part1_;
    unsigned char part2_;
    RecordDelimaType type_;
};

class Tokenizer {
  public:
    //data -- input data, slice1[delima]slice2[delima]slicen[delima]
    //dlen -- data len
    //ret -- -1 error, 0 success
    static int tokenize(char *data, int64_t dlen, char delima, int &token_nr, char *tokens[]);

    static void tokenize(const std::string &str, std::vector<std::string> &result, char delima);

    static void tokenize(const std::string &str, char delima, int &token_nr, TokenInfo *tokens);

    static void tokenize(Slice &data, char delima, int &token_nr, TokenInfo *tokens);

    static void tokenize(Slice &data, const RecordDelima &delima, int &token_nr, TokenInfo *tokens);
	//add by pangtz:20141126 重载tokenize函数，添加TableParam参数
	static void tokenize(Slice &data, const TableParam &table_param, int &token_nr, TokenInfo *tokens);
	//add:end
    static void tokenize_gbk(Slice &slice, char delima, int &token_nr, TokenInfo *tokens);
};

#endif
