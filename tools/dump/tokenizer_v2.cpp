#include "tokenizer_v2.h"
//add by pangtz:20151126
#include "ob_import_param_v2.h"
//add:end

using namespace std;

extern bool trim_end_blank;

bool RecordDelima::is_delima(const char *buff, int64_t pos, int64_t buff_size) const
{
  bool ret = false;

  if (pos < buff_size) {
    if (type_  == CHAR_DELIMA) {
      ret = (buff[pos] == part1_);
    } else {
      if (buff_size > (pos + 1)) {
        ret = buff[pos] == part1_ && buff[pos + 1] == part2_;
      }
    }
  }

  return ret;
}

int Tokenizer::tokenize(char *data, int64_t dlen, char delima, int &token_nr, char *tokens[])
{
  int ret = 0;
  int64_t pos = 0;
  int token = 0;
  char *ptoken = NULL;

  if (!token_nr || !tokens || !data) {
    ret = -1;
//    fprintf(stderr, "paramter must be well inited\n");
    return ret;
  }

  while (pos < dlen && token < token_nr) {
    ptoken = data + pos;
    for ( ; data[pos] != delima && data[pos] != '\n' && data[pos] != '\0' && pos < dlen; pos++);
    data[pos++] = '\0';
//    fprintf(stderr, "token=%s, ", ptoken);
    tokens[token++] = ptoken;
  }

//  fprintf(stderr, "\n");
//  fprintf(stderr, "token_nr=%ld\n", token);

  if (token != token_nr) {
    ret = -1;
  }

  return ret;
}

void Tokenizer::tokenize(const string &str, vector<string> &result, char delima)
{
  size_t pos = 0;
  size_t next_pos = 0;

  while ((next_pos = str.find(delima, pos)) != string::npos) {
    string substr = str.substr(pos, next_pos - pos);
    result.push_back(substr);
    pos = next_pos + 1;                         /* skip delima */
  }

  if (pos != string::npos) {
    string substr = str.substr(pos);
    result.push_back(substr);
  }
}


void Tokenizer::tokenize(const std::string &str, char delima, int &token_nr, TokenInfo *tokens)
{
  size_t pos = 0;
  size_t next_pos = 0;
  int token = 0;

  while ((next_pos = str.find(delima, pos)) != string::npos && token < token_nr) {
    tokens[token].token = str.data() + pos;
    tokens[token].len = next_pos - pos;
    token++;
    pos = next_pos + 1;                         /* skip delima */
  }

  if (pos != string::npos && token < token_nr) {
    tokens[token].token = str.data() + pos;
    tokens[token].len = str.length() - pos;
    token++;
  }

  token_nr = token;
}

void Tokenizer::tokenize_gbk(Slice &slice, char delima, int &token_nr, TokenInfo *tokens)
{
  size_t pos = 0;
  int token = 0;
  const char *ptoken = NULL;
  const char *data = slice.data();

  if (!token_nr || !tokens) {
    token_nr = 0;
    return;
  }

  size_t dlen = slice.size();
  while (pos < dlen && token < token_nr) {
    ptoken = data + pos;

    while(true) {
      if (pos >= dlen) break;
      if (data[pos] == '\n') break;
      if (data[pos] == '\0') break;

      if (static_cast<unsigned char>(data[pos]) >= 128u) { //判断是否是gbk中文字符的前一个编码，如果是，则跳过下一个编码
        pos ++;
      }
      else if (data[pos] == delima) {
        break;
      }
      pos ++;
    }

    tokens[token].token = ptoken;
    tokens[token].len = data + pos - ptoken;
    token++;
    pos++;
  }

  token_nr = token;
}

void Tokenizer::tokenize(Slice &slice, char delima, int &token_nr, TokenInfo *tokens)
{
  size_t pos = 0;
  int token = 0;
  const char *ptoken = NULL;
  const char *data = slice.data();

  if (!token_nr || !tokens) {
    token_nr = 0;
    return;
  }

  size_t dlen = slice.size();
  while (pos < dlen && token < token_nr) {
    ptoken = data + pos;

    for ( ; pos < dlen && data[pos] != delima && data[pos] != '\n' && data[pos] != '\0' ; pos++);

    tokens[token].token = ptoken;
    tokens[token].len = data + pos - ptoken;
    token++;
    pos++;
  }

  token_nr = token;
}

void Tokenizer::tokenize(Slice &slice, const RecordDelima &delima, int &token_nr, TokenInfo *tokens)
{
  int64_t pos = 0;
  int token = 0;
  const char *ptoken = NULL;
  const char *data = slice.data();

  if (!token_nr || !tokens) {
    token_nr = 0;
    return;
  }

  int64_t dlen = static_cast<int64_t>(slice.size());
  while (pos < dlen && token < token_nr) {
    ptoken = data + pos;

    for ( ; pos < dlen && !delima.is_delima(data, pos, dlen); pos++);

    tokens[token].token = ptoken;
    //tokens[token].len = data + pos - ptoken;
    int64_t length = data + pos - ptoken;
    if(trim_end_blank) {
		while(length > 0){
			if(*(ptoken + length - 1) == ' ') {
				length--;
			} else {
				break;
			}
    	}
    }
    tokens[token].len = length;
    token++;
    delima.skip_delima(pos);
  }

  token_nr = token;
}

/*
 * add by pangtz
 * tokenize函数的重载，可以接收TableParam参数
 *
 */
void Tokenizer::tokenize(Slice &slice, const TableParam &table_param, int &token_nr, TokenInfo *tokens)
{
  int64_t pos = 0;
  int token = 0;
  const char *ptoken = NULL;
  const char *data = slice.data();

  if (!token_nr || !tokens) {
    token_nr = 0;
    return;
  }

  int64_t dlen = static_cast<int64_t>(slice.size());
  while (pos < dlen && token < token_nr) {
    ptoken = data + pos;
//modify by pangtz:20141127
    //for ( ; pos < dlen && !delima.is_delima(data, pos, dlen); pos++);
    for ( ; pos < dlen && !table_param.delima.is_delima(data, pos, dlen); pos++);
//add:end

    tokens[token].token = ptoken;
    //tokens[token].len = data + pos - ptoken;
    int64_t length = data + pos - ptoken;

//del by zhuxh:20160926 [trim between two quotation marks] :b
#if 0
	//add by pangtz:20141126 进行trim处理
	switch(table_param.trim_flag) {
		case 1:
			while(length > 0){
				if(*tokens[token].token == ' '){
					tokens[token].token ++;
					length--;
				}else{
					break;
				}
			}
			break;
		case 2:
			while(length > 0){
				if(*(tokens[token].token + length - 1) == ' '){
					length--;
				}else{
					break;
				}
			}
			break;
		case 0:
			while(length > 0){
				if(*tokens[token].token == ' '){
					tokens[token].token ++;
					length--;
				}else{
					break;
				}
			}
			while(length > 0){
				if(*(tokens[token].token + length - 1) == ' '){
					length--;
				}else{
					break;
				}
			}
			break;
		default:
			break;
	}
	//add:end
#endif
//mod by zhuxh:20160926 [trim between two quotation marks] :e

    //mod by zhuxh:20160309 [\0 and \n should be all considered] :b
	//add by pangtz:20141126 删除字段末尾的结束符\0
    while(length > 0)
    {
      char c=*(tokens[token].token + length - 1);
      if( c == '\0' || c == '\n' ){
        length--;
      }else{
        break;
      }
    }
	//add:end
    //mod :e
	
    tokens[token].len = length;
    token++;
//modify by pangtz:20141127
    //delima.skip_delima(pos);
    table_param.delima.skip_delima(pos);
//add:end
  }

  token_nr = token;
}

