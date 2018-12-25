import random
import gen_test_data

class IntObj:
  def __init__(self):
    self.is_add = False
    self.value = 0

  def __init__(self, is_add, value):
    self.is_add = is_add
    self.value = value
  
  def to_string(self):
    if self.is_add:
      return "%d+" % self.value
    else:
      return "%d" % self.value

  def add(self, other):
    if other.is_add:
      self.value += other.value
    else:
      self.value = other.value
    self.is_add = False

def gen_int_obj(rand_is_add = False):
  if rand_is_add:
    is_add = True
    if 0 == random.randint(0, 1):
      is_add = False
    return IntObj(is_add, random.randint(0, 1000))
  else:
    return IntObj(False, random.randint(0, 1000))

class Row:
  DESC = "20 30 40 50 60"
  TYPE = "1 1 0 1 0"

  def __init__(self):
    self.c2 = None
    self.c3 = None
    self.c4 = None
    self.c5 = None
    self.c6 = None
  
  def to_string(self):
    return "%s %s %s %s %s" % (self.c2, self.c3, self.c4.to_string(), self.c5, self.c6.to_string())

class UpsRow(Row):
  def __init__(self):
    Row.__init__(self)
    self.is_delete = False

  def to_string(self):
    t = "+"
    if self.is_delete:
      t = "-"
    return t + " " + Row.to_string(self)

class JoinUpsRow:
  DESC = "20 30 40 50"
  TYPE = "1 1 0 1"

  def __init__(self):
    self.is_delete = False
    self.c2 = None
    self.c3 = None
    self.c4 = None
    self.c5 = None

  def to_string(self):
    t = "+"
    if self.is_delete:
      t = "-"
    return t + " " + "%s %s %s %s" % (self.c2, self.c3, self.c4.to_string(), self.c5)


def gen_row(rowkey_seq, row):
  rowkey = "rowkey_%05d" % (rowkey_seq)
  row.c2 = rowkey;

  #join_rowkey = "rowkey_%05d" % random.randint(0, 100)
  row.c3 = rowkey;

  row.c4 = gen_int_obj() 
  row.c5 = gen_test_data.random_str(5, 10)
  row.c6 = gen_int_obj() 

def gen_join_ups_row(rowkey_seq):
  join_ups_row = JoinUpsRow()
  rowkey = "rowkey_%05d" % (rowkey_seq)
  join_ups_row.is_delete = True
  if 0 == random.randint(0, 1):
    join_ups_row.is_delete = False
  join_ups_row.c2 = rowkey
  join_ups_row.c3 = gen_test_data.random_str(5, 10);
  join_ups_row.c4 = gen_int_obj(True) 
  join_ups_row.c5 = gen_test_data.random_str(5, 10);
  return join_ups_row

def gen_ups_row(rowkey_seq, ups_row):
  rowkey = "rowkey_%05d" % (rowkey_seq)
  ups_row.c2 = rowkey;

  join_rowkey = "rowkey_%05d" % random.randint(0, 100)
  ups_row.c3 = rowkey;

  ups_row.c4 = gen_int_obj(True) 
  ups_row.c5 = gen_test_data.random_str(5, 10)
  ups_row.c6 = gen_int_obj(True) 

  ups_row.is_delete = False
  if 0 == random.randint(0, 1):
    ups_row.is_delete = True
  return ups_row

def gen_join_ups_table(path, row_num):
  table = {}
  for i in range(0, row_num):
    rowkey = "rowkey_%05d" % i
    table[rowkey] = gen_join_ups_row(i)

  f = open(path, 'w')
  print >>f, 1002
  print >>f, row_num, 4
  print >>f, JoinUpsRow.DESC
  print >>f, JoinUpsRow.TYPE
  print >>f, "20"
  print >>f, "20 30 40 50"
  for i in range(0, row_num):
    rowkey = "rowkey_%05d" % i
    if rowkey in table:
      print >>f, table[rowkey].to_string()
  f.close()
  return table

def gen_ups_table1(path, row_num):
  table = {}
  table2 = {}
  for i in range(0, row_num):
    rowkey = "rowkey_%05d" % i
    ups_row = UpsRow()
    gen_ups_row(i, ups_row)
    table2[rowkey + ups_row.c3] = ups_row
    table[rowkey] = ups_row
  f = open(path, 'w')

  print >>f, 1001
  print >>f, row_num, 5
  print >>f, Row.DESC
  print >>f, Row.TYPE
  print >>f, "20 30"
  print >>f, "20 30 40 50 60"

  for i in range(0, row_num):
    rowkey = "rowkey_%05d" % i
    if rowkey in table:
      print >>f, table[rowkey].to_string()
  f.close()
  return table2


def gen_table1(table1_path, row_num):
  table1 = []
  for i in range(0, row_num):
    row = Row()
    gen_row(i, row)
    table1.append(row)

  f = open(table1_path, 'w')
  print >>f, 1001
  print >>f, row_num, 5
  print >>f, Row.DESC
  print >>f, Row.TYPE
  print >>f, "20 30"
  print >>f, "20 30 40 50 60"
  for i in range(0, len(table1)):
    print >>f, table1[i].to_string()
  f.close()
  return table1

def fuse_row(row, ups_row):
  if ups_row.is_delete:
    row.c2 = ups_row.c2
    row.c3 = ups_row.c3
    row.c4 = ups_row.c4
    row.c4.is_add = False
    row.c5 = ups_row.c5
    row.c6 = ups_row.c6
    row.c6.is_add = False
  else:
    row.c2 = ups_row.c2
    row.c3 = ups_row.c3
    row.c4.add(ups_row.c4)
    row.c5 = ups_row.c5
    row.c6.add(ups_row.c6)
  return row 

def fuse_row2(row, join_ups_row):
  if join_ups_row.is_delete:
    row.c5 = join_ups_row.c3
    row.c6 = join_ups_row.c4
  else:
    row.c5 = join_ups_row.c3
    row.c6.add(join_ups_row.c4)

def gen_result(path, table1, ups_table1, ups_table2):
  table = []
  result = None
  for i in range(0, len(table1)):
    rowkey = table1[i].c2 + table1[i].c3
    if rowkey in ups_table1:
      result = fuse_row(table1[i], ups_table1[rowkey])
    else:
      result = table1[i]

    join_rowkey = result.c3
    if join_rowkey in ups_table2:
      fuse_row2(result, ups_table2[join_rowkey])

    table.append(result)

  f = open(path, 'w')
  print >>f, 1001
  print >>f, len(table), 5
  print >>f, Row.DESC
  print >>f, Row.TYPE
  print >>f, "20 30"
  print >>f, "20 30 40 50 60"
  for i in range(0, len(table)):
    print >>f, table[i].to_string()
  f.close()

  return result

if __name__ == '__main__':
  num = 1000
  table1 = gen_table1("tablet_scan_test_data/table1.ini", num)
  ups_table1 = gen_ups_table1("tablet_scan_test_data/ups_table1.ini", num)
  ups_table2 = gen_join_ups_table("tablet_scan_test_data/ups_table2.ini", num)
  gen_result("tablet_scan_test_data/result.ini", table1, ups_table1, ups_table2)

