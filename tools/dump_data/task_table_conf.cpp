#include "task_table_conf.h"
#include "task_utils.h"
#include <string>

using namespace oceanbase::tools;
using namespace oceanbase::common;
using namespace std;

static const char *kTabRowkeyItem = "rowkey.item";
static const char *kTabReviseCol = "revise_column";
static const char *kColumnList = "column_info";
static const char *kTableId = "table.id";
static const char *kTableOutputPath = "table.output";
static const char *kDelima = "delima";
static const char *kRecDelima = "rec_delima";

RowkeyWrapper::RowkeyItemType RowkeyWrapper::RowkeyItem::str_to_type(const char *str)
{
  if (strcmp("INT8", str) == 0)
    return INT8;
  else if (strcmp("INT16", str) == 0)
    return INT16;
  else if (strcmp("INT32", str) == 0)
    return INT32;
  else if (strcmp("INT64", str) == 0)
    return INT64;
  else if (strcmp("STR", str) == 0)
    return STR;
  else if (strcmp("VSTR", str) == 0)
    return VSTR;
  else
    return NONE;
}

int RowkeyWrapper::RowkeyItem::parse_from_string(const char *input_str)
{
#define ITEM_BUFFER_SIZE 128

  int ret = OB_SUCCESS;
  const char *p = NULL;
  const char *delima = ",";

  char str[ITEM_BUFFER_SIZE];
  if (strlen(input_str) > ITEM_BUFFER_SIZE)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "rowkey item is too len, max len is %d", ITEM_BUFFER_SIZE);
  }
  else
  {
    strcpy(str, input_str);
  }

  TBSYS_LOG(DEBUG, "input_str=%s", input_str);
  //parse start_pos
  if (ret == OB_SUCCESS)
  {
    p = strtok(str, delima);
    if (p == NULL)
    {
      TBSYS_LOG(ERROR, "can't parse start_pos");
      ret = OB_ERROR;
    }
    else
    {
      TBSYS_LOG(DEBUG, "start_pos=%s", p);
      start_pos = atoi(p);
    }
  }

  //parse length
  if (ret == OB_SUCCESS)
  {
    p = strtok(NULL, delima);
    if (p == NULL)
    {
      TBSYS_LOG(ERROR, "can't parse length");
      ret = OB_ERROR;
    }
    else
    {
      TBSYS_LOG(DEBUG, "length=%s", p);
      item_len = atoi(p);
    }
  }

  //parse type
  if (ret == OB_SUCCESS)
  {
    p = strtok(NULL, delima);
    if (p == NULL)
    {
      TBSYS_LOG(ERROR, "can't parse type");
      ret = OB_ERROR;
    }
    else
    {
      type = str_to_type(p);
      if (type == NONE)
      {
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(DEBUG, "rkitem type = %d", type);
      }
    }
  }

  //parse rowkey item name
  if (ret == OB_SUCCESS)
  {
    p = strtok(NULL, delima);
    if (p == NULL)
    {
      TBSYS_LOG(ERROR, "can't parse rowkey item name");
      ret = OB_ERROR;
    }
    else
    {
      name = p;
    }
  }

  return ret;
}

//fill items for conf
int RowkeyWrapper::load(const std::vector<const char*> &conf)
{
  int ret = OB_SUCCESS;

  for(size_t i = 0;i < conf.size(); i++)
  {
    RowkeyItem item;
    ret = item.parse_from_string(conf[i]);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can't parse string");
      break;
    }

    items_.push_back(item);
  }

  return ret;
}

//encode rowkey to ObDataBuffer
int RowkeyWrapper::encode(const ObString &rowkey, ObDataBuffer &buff, const RecordDelima &delima) const
{
  int ret = OB_SUCCESS;
  Iterator itr = begin();

  while (itr != end())
  {
    ret = append_rowkey_item(rowkey, *itr, buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can't append rowkey item, start_pos=%d, type=%d",
          itr->start_pos, itr->type);
      break;
    }

    ret = append_delima(buff, delima);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can't append delima, start_pos=%d, type=%d",
          itr->start_pos, itr->type);
      break;
    }

    itr++;
  }

  return ret;
}

int RowkeyWrapper::append_rowkey_item(const ObString &key, const RowkeyItem &item,
                                      ObDataBuffer & buffer) const
{
  int ret = OB_SUCCESS;
  int type = item.type;

  switch(type) {
    case INT64:
    case INT32:
    case INT16:
      {
        int64_t value;
        int key_part_len = 8;
        char val_buf[8];

        if (type == INT16)
        {
          key_part_len = 2;
        }
        else if (type == INT32)
        {
          key_part_len = 4;
        }

        if (key.length() < (item.start_pos + item.item_len) ||
            item.item_len != key_part_len)
        {
          TBSYS_LOG(ERROR, "rowkey item config error, start_pos=%d, type=%d, key_part_len = %d",
              item.start_pos, item.type, key_part_len);
          ret = OB_ERROR;
          break;
        }

        memset(val_buf, 0, 8);
        memcpy(val_buf, key.ptr() + item.start_pos, key_part_len);
        reverse(val_buf, val_buf + key_part_len);
        memcpy(&value, val_buf, 8);

        ret = append_int64(value, buffer);
      }
      break;
    case INT8:
      {
        int8_t value;
        if (key.length() < (item.start_pos + 1) ||
            item.item_len != 1)
        {
          TBSYS_LOG(ERROR, "rowkey item config error, start_pos=%d, type=%d",
              item.start_pos, item.type);
          ret = OB_ERROR;
          break;
        }

        value = key.ptr()[item.start_pos];
        ret = append_int8(value, buffer);
        break;
      }
      break;
    case STR:
      {
        if (key.length() < item.start_pos + item.item_len)
        {
          TBSYS_LOG(ERROR, "configuration error, item.start_pos=%d, item.item_len=%d",
              item.start_pos, item.item_len);
          break;
        }

        ret = append_str(key.ptr() + item.start_pos, item.item_len, buffer);
      }
      break;
    case VSTR:
      {
        if (key.length() < item.start_pos)
        {
          TBSYS_LOG(ERROR, "configuration error, item.start_pos=%d",
              item.start_pos);
          break;
        }

        int64_t len = key.length() - item.start_pos;
        ret = append_str(key.ptr() + item.start_pos, len, buffer);
      }
      break;
    default:
      TBSYS_LOG(ERROR, "such type [%d] is not supported now", type);
      break;
  }

  return ret;
}

TableConf::TableConf(const std::vector<const char *> &revise_columns,
                     const std::vector<const char *> &rowkey_items,
                     const std::vector<const char *> &columns,
                     const char *table_name,
                     int64_t table_id) :
  revise_cols_(revise_columns), table_id_(table_id), delima_(1), rec_delima_(10)
{
  columns_ = columns;
  table_name_ = table_name;

  int ret = rowkey_wrappper_.load(rowkey_items);
  if (ret == OB_SUCCESS)
  {
    valid_ = true;
  }
  else
  {
    TBSYS_LOG(ERROR, "can't load rowkey items, please your config");
  }
}

bool TableConf::is_revise_column(const ObString &col) const
{
  bool ret = false;

  for(size_t i = 0;i < revise_cols_.size(); i++)
  {
    if (col.compare(revise_cols_[i]) == 0)
    {
      ret = true;
      break;
    }
  }

  return ret;
}

int TableConf::revise_column(const ObCellInfo &cell, ObDataBuffer &data) const
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  //convert int64 to price, price = int64 / 1000
  ret = cell.value_.get_int(tmp);

  if (ret != OB_SUCCESS && (cell.value_.get_type() == ObNullType)) { /* check if it is a null type */
    tmp = 0;
    ret = OB_SUCCESS;
  }

  if (ret == OB_SUCCESS)
  {
    double value = (double)tmp / 100;

    ret = append_double(value, data);
    if (ret != OB_SUCCESS)
    {
      // TODO
      // string column(cell.row_key_.ptr(), cell.row_key_.length());
      // TBSYS_LOG(ERROR, "append_double error, column:%s", column.c_str());
    }
  }
  else
  {
    string column(cell.column_name_.ptr(), cell.column_name_.length());
    char buf[128];

    //hex_to_str will return 0, if buffer size is not enough
    int len = hex_to_str(cell.row_key_.ptr(), (int)cell.row_key_.length(), buf, 128);
    buf[2 * len] = 0;
    TBSYS_LOG(ERROR, "obcellinfo get value failed, column=%s, rowkey=%s, ret=%d" ,column.c_str(), buf, ret);
  }

  return ret;
}

bool TableConf::is_rowkey(const char *column) const
{
  bool ret = false;
  RowkeyWrapper::Iterator itr = rowkey_wrappper_.begin();

  while (itr != rowkey_wrappper_.end())
  {
    if (strcmp(column, itr->name.c_str()) == 0)
    {
      ret = true;
      break;
    }
    itr++;
  }

  return ret;
}

int TableConf::encode_rowkey(const ObString &rowkey, ObDataBuffer &data) const
{
  return rowkey_wrappper_.encode(rowkey, data, delima_);
}

int TableConf::append_rowkey_item(const ObString &key, const char *rk_item_name,
                       ObDataBuffer & buffer) const
{
  int ret = OB_SUCCESS;

  RowkeyWrapper::Iterator itr = rowkey_wrappper_.begin();
  while (itr != rowkey_wrappper_.end())
  {
    if (itr->name == rk_item_name)
    {
      break;
    }
    itr++;
  }

  if (itr == rowkey_wrappper_.end())
  {
    TBSYS_LOG(ERROR, "No such rowkey column %s, please check it", rk_item_name);
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS)
  {
    ret = rowkey_wrappper_.append_rowkey_item(key, *itr, buffer);
  }

  return ret;
}

int TableConf::loadConf(const char *table_name, TableConf &conf)
{
  int ret = OB_SUCCESS;
  vector<const char *> rowkey_items =
    TBSYS_CONFIG.getStringList(table_name, kTabRowkeyItem);
  vector<const char *> revise_cols =
    TBSYS_CONFIG.getStringList(table_name, kTabReviseCol);
  vector<const char *> columns =
    TBSYS_CONFIG.getStringList(table_name, kColumnList);

  int64_t table_id = TBSYS_CONFIG.getInt(table_name, kTableId);
  TableConf tmp_conf(revise_cols, rowkey_items, columns, table_name, table_id);
  if (tmp_conf.valid())
  {
    const char *out = TBSYS_CONFIG.getString(table_name, kTableOutputPath);
    tmp_conf.set_output_path(out);

    const char *str_delima =
      TBSYS_CONFIG.getString(table_name, kDelima);
    if (str_delima != NULL)
    {
      RecordDelima delima;
      const char *end_pos = str_delima + strlen(str_delima);

      if (find(str_delima, end_pos, ',') == end_pos)
      {
        delima = RecordDelima(static_cast<char>(atoi(str_delima)));
      }
      else
      {
        int part1, part2;

        sscanf(str_delima, "%d,%d", &part1, &part2);
        delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
      }

      tmp_conf.set_delima(delima);
    }

    const char *str_rec_delima =
      TBSYS_CONFIG.getString(table_name, kRecDelima);
    if (str_rec_delima != NULL)
    {
      RecordDelima rec_delima;
      const char *end_pos = str_rec_delima + strlen(str_delima);

      if (find(str_rec_delima, end_pos, ',') == end_pos)
      {
        rec_delima = RecordDelima(static_cast<char>(atoi(str_rec_delima)));
      }
      else
      {
        int part1, part2;

        sscanf(str_rec_delima, "%d,%d", &part1, &part2);
        rec_delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
      }

      tmp_conf.set_rec_delima(rec_delima);
    }

    conf = tmp_conf;
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "invalid table conf finded, table:%s", table_name);
  }

  return ret;
}

std::string TableConf::DebugString() const
{
  char buf[128];
  snprintf(buf, 128,"table_id=%ld,table_name=%s\n", table_id(), table_name_.c_str());
  string res = buf;
  snprintf(buf, 128, "delima type = %d, part1 = %d, part2 = %d\n", delima_.type_, delima_.part1_, delima_.part2_);
  res += buf;
  snprintf(buf, 128, "rec delima type = %d, part1 = %d, part2 = %d\n", rec_delima_.type_, rec_delima_.part1_, rec_delima_.part2_);
  res += buf;
  res += "rowkey=";
  for(size_t i = 0;i < rowkey_wrappper_.items_.size(); i++) {
    snprintf(buf, 128, "[start_pos=%d, len=%d, type=%d, name=%s]",
             rowkey_wrappper_.items_[i].start_pos,
             rowkey_wrappper_.items_[i].item_len,
             rowkey_wrappper_.items_[i].type,
             rowkey_wrappper_.items_[i].name.c_str());
    res += buf;
  }
  res += "\nrevise_column=";

  for(size_t i = 0;i < revise_cols_.size(); i++) {
    res +="[";
    res += revise_cols_[i];
    res += "]";
  }

  res += "\ncolumns={";
  for(size_t i = 0;i < columns_.size(); i++) {
    res += "[";
    res += columns_[i];
    res += "]";
  }
  res += "}\n";

  return res;
}
