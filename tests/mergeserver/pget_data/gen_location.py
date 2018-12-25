#!/usr/bin/env python
import os
import sys

table_count = 10
min_table_id = 1001
location_f = open("./location_cache.txt", "w")

location_f.write("[tablet_location_cache]\n\n")

svrs = [["10.232.35.1", "10.232.35.2", "10.232.35.3"], 
        ["10.232.35.4", "10.232.35.5", "10.232.35.6"],
        ["10.232.35.7", "10.232.35.8", "10.232.35.9"],
        ["10.232.35.10", "10.232.35.11", "10.232.35.12"]]

svr_g_count = len(svrs)

for i in xrange(table_count):
  location_f.write("table_id_%d = %d\n"%(i, min_table_id + i))
  location_f.write("range_start_%d = MIN\n"%(i))
  location_f.write("range_end_%d = MAX\n"%(i))
  location_f.write("server_list_%d = %s\n\n\n"%(i, ",".join(svrs[i%svr_g_count])))
