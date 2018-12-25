#ifndef OB_COLUMN_CHEKSUM_H
#define OB_COLUMN_CHEKSUM_H
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <openssl/md5.h>
//add wenghaixing [secondary index col checksum] 20141204
//static const int64_t OB_MAX_COL_CHECKSUM_STR_LEN=9082;
#include <map>
#include <vector>
#include <string.h>
#include "utility.h"

//add e
namespace oceanbase
{
    namespace common
    {
    //add wenghaixing [secondary index col checksum] 20141210
        static const int64_t OB_MAX_COL_CHECKSUM_COLUMN_COUNT=100;
        static const int64_t OB_MAX_COL_CHECKSUM_STR_LEN=2560;
        const char* const OB_FOR_NULL_POINTER ="";
        struct Token
        {
            Token() : token(NULL), len(0) { }
            const char *token;
            int64_t len;
        };
        class col_checksum
        {

        public:
          char* get_str()
          {
              return checksum_str;
          }
          const char* get_str_const() const
          {
              return checksum_str;
          }

        //add wenghaixing [secondary index col checksum] 20141208
           //add wenghaixing [secondary index col checksum] 20141208
         int sum (const col_checksum &col)
              {
                  int ret = OB_SUCCESS;

                  //add liumz, [bugfix: empty tablet]20161121:b
                  if (0 == strlen(col.get_str_const()))
                  {
                    TBSYS_LOG(WARN, "empty column checksum!");
                  }
                  //add:e
                  else if (0 == strlen(checksum_str))
                  {
                      strcpy(checksum_str, col.checksum_str);
                  }
                  else
                  {
                      //首先解析自己的checksum_str
                      int token_nr_src = OB_MAX_COL_CHECKSUM_COLUMN_COUNT;
                      Token tokens_src[OB_MAX_COL_CHECKSUM_COLUMN_COUNT];

                      int token_nr_add = OB_MAX_COL_CHECKSUM_COLUMN_COUNT;
                      Token tokens_add[OB_MAX_COL_CHECKSUM_COLUMN_COUNT];

                      if (OB_SUCCESS != (ret = tokenize(checksum_str, strlen(checksum_str), ',', token_nr_src, tokens_src)))
                      {
                          TBSYS_LOG(ERROR, "failed to parse src column checksum_str[%s]", checksum_str);
                          ret = OB_ERROR;
                      }
                      else if (OB_SUCCESS != (ret = tokenize(col.checksum_str, strlen(col.checksum_str), ',', token_nr_add, tokens_add)))
                      {
                          TBSYS_LOG(ERROR, "failed to parse add column checksum_str[%s]", col.checksum_str);
                          ret = OB_ERROR;
                      }
                      else if (token_nr_src != token_nr_add)
                      {
                          TBSYS_LOG(ERROR, "column checksum count is not equal, src column count[%d], add column count[%d], src column checksum_str[%s], add column checksum_str[%s]",
                                              token_nr_src, token_nr_add, checksum_str, col.checksum_str);
                          ret = OB_ERROR;
                      }
                      else
                      {
                          int idx = 0;
                          std::map<uint64_t, uint64_t> col_sum_add;//存储add column checksum_str中所有<cid, checksum>
                          for (idx = 0; idx < token_nr_add && OB_SUCCESS == ret; idx++)
                          {
                              int attr_nr = 2;
                              Token tokens_attr[2];
                              if (0 != (ret = tokenize(tokens_add[idx].token, tokens_add[idx].len, ':', attr_nr, tokens_attr)))
                              {
                                  TBSYS_LOG(ERROR, "fialed to separate cid:checksum[%.*s], add column checksum_str[%s]", static_cast<int>(tokens_add[idx].len), tokens_add[idx].token, col.checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else if (attr_nr != 2)
                              {
                                  TBSYS_LOG(ERROR, "column checksum must be 2 attributes, in manner of <cid:checksum>, column checksum[%.*s], add column checksum_str[%s]", static_cast<int>(tokens_add[idx].len), tokens_add[idx].token, col.checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else
                              {
                                  uint64_t cid = 0;
                                  uint64_t checksum = 0;
                                  if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[0].token, tokens_attr[0].len, cid)))
                                  {
                                      TBSYS_LOG(ERROR, "fetch column id failed, column id str[%.*s], add column checksum_str[%s]", static_cast<int>(tokens_attr[0].len), tokens_attr[0].token, col.checksum_str);
                                      ret = OB_ERROR;
                                      break;
                                  }
                                  else if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[1].token, tokens_attr[1].len, checksum)))
                                  {
                                      TBSYS_LOG(ERROR, "fetch checksum  failed, checksum str[%.*s], add column checksum_str[%s]", static_cast<int>(tokens_attr[1].len), tokens_attr[1].token, col.checksum_str);
                                      ret = OB_ERROR;
                                      break;
                                  }
                                  else
                                  {
                                      std::map<uint64_t, uint64_t>::iterator iter = col_sum_add.find(cid);
                                      if (iter != col_sum_add.end())
                                      {
                                          TBSYS_LOG(ERROR, "equal column[cid = %lu] exists in add column checksum_str[%s]", cid, col.checksum_str);
                                          ret = OB_ERROR;
                                          break;
                                      }
                                      else
                                      {
                                          col_sum_add.insert(std::make_pair(cid, checksum));
                                      }
                                  }
                              }
                          }

                          std::vector<std::pair<uint64_t, uint64_t> > col_sum_src;//存储src column checksum中所有<cid,checksum>
                          for (idx = 0; idx < token_nr_src && OB_SUCCESS == ret; idx++)
                          {
                              int attr_nr = 2;
                              Token tokens_attr[2];
                              if (OB_SUCCESS != (ret = tokenize(tokens_src[idx].token, tokens_src[idx].len, ':', attr_nr, tokens_attr)))
                              {
                                  TBSYS_LOG(ERROR, "fialed to separate cid:checksum[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_src[idx].len), tokens_src[idx].token, checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else if (attr_nr != 2)
                              {
                                  TBSYS_LOG(ERROR, "column checksum must be 2 attributes, in manner of <cid:checksum>, column checksum[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_src[idx].len), tokens_src[idx].token, checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else
                              {
                                  uint64_t cid = 0;
                                  uint64_t checksum = 0;
                                  if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[0].token, tokens_attr[0].len, cid)))
                                  {
                                      TBSYS_LOG(ERROR, "fetch column id failed, column id str[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_attr[0].len), tokens_attr[0].token, checksum_str);
                                      ret = OB_ERROR;
                                      break;
                                  }
                                  else if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[1].token, tokens_attr[1].len, checksum)))
                                  {
                                      TBSYS_LOG(ERROR, "fetch checksum  failed, checksum str[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_attr[1].len), tokens_attr[1].token, checksum_str);
                                      ret = OB_ERROR;
                                      break;
                                  }
                                  else
                                  {
                                      //找出cid在col_sum_add中对应的值,并与checksum相加,如果找不到报错
                                      std::map<uint64_t, uint64_t>::iterator iter = col_sum_add.find(cid);
                                      if (iter != col_sum_add.end())
                                      {
                                          checksum += iter->second;
                                          col_sum_src.push_back(std::make_pair(cid, checksum));
                                      }
                                      else
                                      {
                                          TBSYS_LOG(ERROR, "can't find equal column id[cid = %lu], src column checksum_str[%s], add column checksum_str[%s]", cid, checksum_str, col.checksum_str);
                                          ret = OB_ERROR;
                                          break;
                                      }
                                  }
                              }
                          }

                          if (OB_SUCCESS == ret)
                          {
                              //生成新的checksum_str
                              int pos = 0;
                              for (size_t idx = 0; idx < col_sum_src.size(); idx++)
                              {
                                  int length = snprintf(checksum_str + pos, OB_MAX_COL_CHECKSUM_STR_LEN, "%lu:%lu", col_sum_src[idx].first, col_sum_src[idx].second);
                                  pos += length;
                                  checksum_str[pos] = ',';
                                  pos++;
                              }
                              pos--;
                              checksum_str[pos] = '\0';
                          }
                      }
                  }

                  return ret;
              }

              bool compare(const col_checksum &col)
              {
                  bool is_equal = false;
                  int ret = OB_SUCCESS;

                  if (!strlen(checksum_str) || !strlen(col.checksum_str))
                  {
                      if (!strlen(checksum_str) && !strlen(col.checksum_str)) is_equal = true;

                      return is_equal;
                  }

                  int token_nr_src = OB_MAX_COL_CHECKSUM_COLUMN_COUNT;
                  Token tokens_src[OB_MAX_COL_CHECKSUM_COLUMN_COUNT];

                  int token_nr_cmp = OB_MAX_COL_CHECKSUM_COLUMN_COUNT;
                  Token tokens_cmp[OB_MAX_COL_CHECKSUM_COLUMN_COUNT];

                  if (OB_SUCCESS != (ret = tokenize(checksum_str, strlen(checksum_str), ',', token_nr_src, tokens_src)))
                  {
                      TBSYS_LOG(ERROR, "failed to parse src column checksum_str[%s]", checksum_str);
                      ret = OB_ERROR;
                  }
                  else if (OB_SUCCESS != (ret = tokenize(col.checksum_str, strlen(col.checksum_str), ',', token_nr_cmp, tokens_cmp)))
                  {
                      TBSYS_LOG(ERROR, "failed to parse compare column checksum_str[%s]", col.checksum_str);
                      ret = OB_ERROR;
                  }
                  else
                  {
                      int idx = 0;
                      /* parse object-self checksum_str*/
                      std::map<uint64_t, uint64_t> col_sum_src;
                      for (idx = 0; idx < token_nr_src && OB_SUCCESS == ret; idx++)
                      {
                          int attr_nr = 2;
                          Token tokens_attr[2];
                          if (OB_SUCCESS != (ret = tokenize(tokens_src[idx].token, tokens_src[idx].len, ':', attr_nr, tokens_attr)))
                          {
                              TBSYS_LOG(ERROR, "fialed to separate cid:checksum[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_src[idx].len), tokens_src[idx].token, checksum_str);
                              ret = OB_ERROR;
                              break;
                          }
                          else if (attr_nr != 2)
                          {
                              TBSYS_LOG(ERROR, "column checksum must be 2 attributes, in manner of <cid:checksum>, column checksum[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_src[idx].len), tokens_src[idx].token, checksum_str);
                              ret = OB_ERROR;
                              break;
                          }
                          else
                          {
                              uint64_t cid = 0;
                              uint64_t checksum = 0;
                              if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[0].token, tokens_attr[0].len, cid)))
                              {
                                  TBSYS_LOG(ERROR, "fetch column id failed, column id str[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_attr[0].len), tokens_attr[0].token, checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[1].token, tokens_attr[1].len, checksum)))
                              {
                                  TBSYS_LOG(ERROR, "fetch checksum  failed, checksum str[%.*s], src column checksum_str[%s]", static_cast<int>(tokens_attr[1].len), tokens_attr[1].token, checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else
                              {
                                  std::map<uint64_t, uint64_t>::iterator iter = col_sum_src.find(cid);
                                  if (iter != col_sum_src.end())
                                  {
                                      TBSYS_LOG(ERROR, "equal column[cid = %lu] exists in src column checksum_str[%s]", cid, checksum_str);
                                      ret = OB_ERROR;
                                      break;
                                  }
                                  else
                                  {
                                      col_sum_src.insert(std::make_pair(cid, checksum));
                                  }
                              }
                          }
                      }
                      /* parse compare parameter checksum_str*/
                      std::map<uint64_t, uint64_t> col_sum_cmp;
                      for (idx = 0; idx < token_nr_cmp && OB_SUCCESS == ret; idx++)
                      {
                          int attr_nr = 2;
                          Token tokens_attr[2];
                          if (OB_SUCCESS != (ret = tokenize(tokens_cmp[idx].token, tokens_cmp[idx].len, ':', attr_nr, tokens_attr)))
                          {
                              TBSYS_LOG(ERROR, "fialed to separate cid:checksum[%.*s], compare column checksum_str[%s]", static_cast<int>(tokens_cmp[idx].len), tokens_cmp[idx].token, col.checksum_str);
                              ret = OB_ERROR;
                              break;
                          }
                          else if (attr_nr != 2)
                          {
                              TBSYS_LOG(ERROR, "column checksum must be 2 attributes, in manner of <cid:checksum>, column checksum[%.*s], compare column checksum_str[%s]", static_cast<int>(tokens_cmp[idx].len), tokens_cmp[idx].token, col.checksum_str);
                              ret = OB_ERROR;
                              break;
                          }
                          else
                          {
                              uint64_t cid = 0;
                              uint64_t checksum = 0;
                              if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[0].token, tokens_attr[0].len, cid)))
                              {
                                  TBSYS_LOG(ERROR, "fetch column id failed, column id str[%.*s], compare column checksum_str[%s]", static_cast<int>(tokens_attr[0].len), tokens_attr[0].token, col.checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else if (OB_SUCCESS != (ret = transform_str_to_int(tokens_attr[1].token, tokens_attr[1].len, checksum)))
                              {
                                  TBSYS_LOG(ERROR, "fetch checksum  failed, checksum str[%.*s], compare column checksum_str[%s]", static_cast<int>(tokens_attr[1].len), tokens_attr[1].token, col.checksum_str);
                                  ret = OB_ERROR;
                                  break;
                              }
                              else
                              {
                                  std::map<uint64_t, uint64_t>::iterator iter = col_sum_cmp.find(cid);
                                  if (iter != col_sum_cmp.end())
                                  {
                                      TBSYS_LOG(ERROR, "equal column[cid = %lu] exists in compare column checksum_str[%s]", cid, col.checksum_str);
                                      ret = OB_ERROR;
                                      break;
                                  }
                                  else
                                  {
                                      col_sum_cmp.insert(std::make_pair(cid, checksum));
                                  }
                              }
                          }
                      }
                      /* compare: finding whether lesser is the subset of higher */
                      if (OB_SUCCESS == ret)
                      {
                          if (col_sum_cmp.size() <= col_sum_src.size())
                          {
                               std::map<uint64_t, uint64_t>::iterator iter = col_sum_cmp.begin();
                               while (iter != col_sum_cmp.end())
                               {
                                    std::map<uint64_t, uint64_t>::iterator iter_temp = col_sum_src.find(iter->first);
                                    if (iter_temp != col_sum_src.end())
                                    {
                                          if (iter->second != iter_temp->second)
                                          {
                                               TBSYS_LOG(ERROR, "equal column id[cid = %lu], but not equal checksum, src column checksum_str[%s], compare column checksum_str[%s]", iter->first, checksum_str, col.checksum_str);
                                               ret = OB_ERROR;
                                               break;
                                          }
                                    }
                                    else
                                    {
                                           TBSYS_LOG(ERROR, "can't find equal column id[cid = %lu], src column checksum_str[%s], compare column checksum_str[%s]", iter->first, checksum_str, col.checksum_str);
                                           ret = OB_ERROR;
                                           break;
                                    }
                                    iter++;
                               }
                          }
                          else
                          {
                               std::map<uint64_t, uint64_t>::iterator iter = col_sum_src.begin();
                               while (iter != col_sum_src.end())
                               {
                                    std::map<uint64_t, uint64_t>::iterator iter_temp = col_sum_cmp.find(iter->first);
                                    if (iter_temp != col_sum_cmp.end())
                                    {
                                          if (iter->second != iter_temp->second)
                                          {
                                               TBSYS_LOG(ERROR, "equal column id[cid = %lu], but not equal checksum, src column checksum_str[%s], compare column checksum_str[%s]", iter->first, checksum_str, col.checksum_str);
                                               ret = OB_ERROR;
                                               break;
                                          }
                                    }
                                    else
                                    {
                                           TBSYS_LOG(ERROR, "can't find equal column id[cid = %lu], src column checksum_str[%s], compare column checksum_str[%s]", iter->first, checksum_str, col.checksum_str);
                                           ret = OB_ERROR;
                                           break;
                                    }
                                    iter++;
                               }
                          }
                      }
                  }

                  if (OB_SUCCESS == ret) is_equal = true;

                  return is_equal;
              }

      void deepcopy(const char* col)
      {
          reset();
          if(col!=NULL)
          {
            strncpy(checksum_str,col,strlen(col));
          }
          if(static_cast<int64_t>(strlen(col))<OB_MAX_COL_CHECKSUM_STR_LEN-1)
          {
              //char e[1]='\0';
              checksum_str[strlen(col)]='\0';
          }
      }

      void deepcopy(const char* col,const int32_t length)
      {
        reset();
        if(col!=NULL)
        {
          strncpy(checksum_str,col,length);
        }
        if(static_cast<int64_t>(strlen(col))<OB_MAX_COL_CHECKSUM_STR_LEN-1)
        {
          //char e[1]='\0';
          checksum_str[strlen(col)]='\0';
        }
      }

      void reset()
      {
          memset(checksum_str,0,OB_MAX_COL_CHECKSUM_STR_LEN);
      }

      col_checksum()
      {
          memset(checksum_str,0,OB_MAX_COL_CHECKSUM_STR_LEN);
      }
      private:
        char checksum_str[OB_MAX_COL_CHECKSUM_STR_LEN];
        };



    }
  }

#endif // OB_COLUMN_CHEKSUM_H
