import random
import string
import sys

class ObObj:
  pass

def random_char():
  return string.letters[random.randint(0, len(string.letters) - 1)]

def random_str(min_len, max_len):
  str = ""
  str_len = random.randint(min_len, max_len)
  for i in range(0, str_len):
    str += random_char()
  return str

def gen_str(row_num, min_len, max_len):
  column = []
  for i in range(0, row_num):
    if 0 == random.randint(0, 4):
      column.append("NULL")
    else:
      column.append(random_str(min_len, max_len))
  return column

def gen_int(row_num, min_int, max_int):
  column = []
  for i in range(0, row_num):
    if 0 == random.randint(0, 4):
      column.append("NULL")
    else:
      int_value = random.randint(min_int, max_int)
      column.append(int_value)
  return column

def gen_table(row_num, column_num):
  table = []
  typelist = []
  typelist.append(1)

  rowkey = []
  row_index = 0
  for i in range(0, row_num):
    row_index += random.randint(1, 5)
    rowkey.append( "rowkey_%05d" % row_index )
  table.append(rowkey)

  for i in range(0, column_num):
    t = random.randint(0, 1)
    typelist.append(t)
    if 0 == t:
      table.append(gen_int(row_num, -1000, 1000))
    else:
      table.append(gen_str(row_num, 5, 10))
  return table, typelist

def convert_table(table):
  out_table = []
  column_num = len(table)
  if column_num < 0:
    return
  row_num = len(table[0])
  for i in range(0, row_num):
    row = []
    for j in range(0, column_num):
      row.append(table[j][i]) 
    out_table.append(row)
  return out_table


def print_table(file_name, table, typelist):
  f = open(file_name, "w")
  print >>f, 1000
  column_num = len(table)
  if column_num < 0:
    return
  row_num = len(table[0])

  print >>f, row_num, column_num

  column_ids = ""
  line = ""
  for i in range(0, len(typelist)):
    column_ids += str(i + 1) + " "
    line += str(typelist[i]) + " "

  column_ids.strip()
  print >>f, column_ids
  line.strip()
  print >>f, line

  print >>f, column_ids.split()[0]
  print >>f, string.join(column_ids.split()[0:], " ")


  for i in range(0, row_num):
    line = ""
    for j in range(0, column_num):
      line += str(table[j][i]) + " "
    line.strip()
    print >>f, line
  f.close()

def convert(tokens, types):
  column = []
  for i in range(0, len(tokens)):
    if tokens[i] == "NULL":
      column.append(None)
    elif "1" == types[i]:
      column.append(tokens[i])
    else:
      column.append(int(tokens[i]))
  return column


def gen_obj(t):
  obj = ObObj()
  if 0 == random.randint(0,1):
    obj.value = "NOP"
  else:
    if "1" == t:
      obj.value = random_str(5, 10)
    else:
      if 0 == random.randint(0, 1):
        obj.add = False
      else:
        obj.add = True
      obj.value = random.randint(-1000, 1000)
  return obj

def merge(origin, column, types):
  ret = []
  for i in range(1, len(origin)):
    j = i
    if column[j] == None:
      ret.append(origin[i])
    else:
      if column[j].value == "NOP":
        if column[0]:
          ret.append("NULL")
        else:
          ret.append(origin[i])
      else:
        if "1" == types[i]:
          ret.append(column[j].value)
        else:
          if column[0]:
            ret.append(column[j].value)
          else:
            if column[j].add:
              if origin[i] == None:
                ret.append(column[j].value)
              else:
                ret.append(origin[i] + column[j].value)
            else:
              ret.append(column[j].value)
  return ret

def obj_to_string(obj, is_ups):
  if not is_ups and obj == None:
    return "NULL"
  elif not is_ups and obj.value == "NOP":
    return "NULL"
  else:
    if isinstance(obj.value, str):
      return obj.value
    else:
      if obj.add:
        if is_ups:
          return "%d+" % (obj.value)
        else:
          return "%d" % (obj.value)
      else:
        return str(obj.value)

def convert_bool(b):
  if b:
    return "-"
  else:
    return "+"

def gen_ups_line(column_count, types, ups_column_ids):
  column = []
  if 0 == random.randint(0, 1):
    column.append(True)
  else:
    column.append(False)
  for i in range(0, column_count):
    if i+2 in ups_column_ids:
      column.append(gen_obj(types[i+1]))
    else:
      column.append(None)
  return column


def to_str(x):
  if x == None:
    return "NULL"
  else:
    return str(x)

def remove_none(column):
  ret = []
  for i in column:
    if i != None:
      ret.append(i)
  return ret

def get_types(types, ups_column_ids):
  ret = []
  for i in ups_column_ids:
    ret.append(types[i-1])
  return ret

def gen_ups_column_ids(gen_all, column_count):
  ret = []
  if gen_all:
    return range(2, column_count - 1 + 2)
  else:
    for i in range(2, column_count - 1 + 2):
      if random.randint(0, 100) > 50:
        ret.append(i)
  if len(ret) == 0:
    ret.append(2)
  return ret

def write_join(ups_column_ids, file_name):
  f = open(file_name, "w")
  print >>f, len(ups_column_ids)
  for i in ups_column_ids:
    print >>f, "%d %d" % (i, i)
  f.close()

def gen_ups(file_name, ups_data_file_name, result_file_name, ups_table_id, for_join):
  
  ups_data_file = open(ups_data_file_name, "w")
  result_file = open(result_file_name, "w")
  f = open(file_name, "r")

  line = f.readline()
  table_id = line.strip()
  print >>result_file, table_id
  print >>ups_data_file, ups_table_id

  #line = f.readline() #jump rowkey define
  line = f.readline()
  tmp = line.strip().split(" ")
  row_count = int(tmp[0])
  column_count = int(tmp[1])

  ups_column_ids = gen_ups_column_ids(not for_join, column_count)

  line = f.readline()
  column_ids_line = line.strip()

  line = f.readline()
  type_line = line.strip()
  types = line.strip().split(" ");

  f.readline() # skip line
  f.readline() # skip line

  ups_data_lines = []
  result_lines = []

  count = 0
  rowkey = "rowkey_%05d" % (count)
    
  for row in range(0, row_count):
    line = f.readline().strip()
    tokens = line.split(" ")
    origin = convert(tokens, types)

    while origin[0] != rowkey:
      count += 1
      rowkey = "rowkey_%05d" % (count)

      if origin[0] != rowkey:
        randint = random.randint(0, 9)
        if randint > 7:
          column = gen_ups_line(column_count, types, ups_column_ids)
          ups_data_lines.append("%s %s %s" % (convert_bool(column[0]), rowkey, string.join(map(lambda x:obj_to_string(x, True), remove_none(column[1:])), " ")))
          if not for_join:
            result_lines.append( "%s %s" % (rowkey, string.join(map(lambda x:obj_to_string(x, False), column[1:-1]), " ")))

    randint = random.randint(0, 9)
    if randint > 3:
      column = gen_ups_line(column_count, types, ups_column_ids)
      ups_data_lines.append("%s %s %s" % (convert_bool(column[0]), tokens[0], string.join(map(lambda x:obj_to_string(x, True), remove_none(column[1:-1])), " ")))
      result_lines.append( "%s %s" % (tokens[0], string.join(map(to_str, merge(origin, column, types)), " ")))
    else:
      result_lines.append(line)

  print >>result_file, len(result_lines), column_count
  print >>result_file, column_ids_line
  print >>result_file, type_line

  print >>result_file, column_ids_line.split()[0]
  print >>result_file, string.join(column_ids_line.split()[0:], " ")


  for i in result_lines:
    print >>result_file, i

  print >>ups_data_file, len(ups_data_lines), len(ups_column_ids) + 1
  print >>ups_data_file, "1 %s" % (string.join(map(lambda x:str(x), ups_column_ids), " ")) 
  print >>ups_data_file, "1 %s" % (string.join(get_types(types, ups_column_ids))) 

  print >>ups_data_file, 1
  print >>ups_data_file, "1 %s" % (string.join(map(lambda x:str(x), ups_column_ids), " ")) 

  for i in ups_data_lines:
    print >>ups_data_file, i

  f.close()
  ups_data_file.close()
  result_file.close()

  return ups_column_ids

def gen_scan(file_name):
  table, typelist = gen_table(1000, 30)
  print_table(file_name, table, typelist)
 
if __name__ == '__main__':
  if len(sys.argv) != 2:
    print "usage: gen_test_data.py [fuse|join]"
  elif sys.argv[1] == "fuse":
    gen_scan("tablet_fuse_test_data/big_sstable.ini")
    gen_ups("tablet_fuse_test_data/big_sstable.ini", "tablet_fuse_test_data/big_ups_scan.ini", 
      "tablet_fuse_test_data/big_result.ini", 1000, False)
  elif sys.argv[1] == "join":
    gen_scan("tablet_join_test_data/big_sstable2.ini")
    ups_column_ids = gen_ups("tablet_join_test_data/big_sstable2.ini", "tablet_join_test_data/big_ups_scan2.ini", 
      "tablet_join_test_data/big_result2.ini", 1001, True)
    write_join(ups_column_ids, "tablet_join_test_data/join_info.ini")
  else:
    print "usage: gen_test_data.py [fuse|join]"

