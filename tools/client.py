import os
import re
import collections
import sqlite3
import types
import sys

if __name__ == "__main__":
  connection = sqlite3.connect("./profile.db")
  cursor = connection.cursor()
  cursor.execute(sys.argv[1])
  res = cursor.fetchall()
  col_name_list = [tuple[0] for tuple in cursor.description]
  for col_name in col_name_list:
    print "%20s %s" % (col_name, "|"),
  print
  for line in res:
    for value in line:
      print "%20s %s" % (value, "|"),
    print

