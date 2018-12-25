#!/usr/bin/env python
import os
import sys
import re
import optparse
import textwrap
import shutil
import time
from optparse import OptionParser

def main():
  parser = OptionParser()  
  parser.add_option("-f", "--file", action="store", dest = "range_file", help = "input range file")  
  parser.add_option("-o", "--output", action = "store", dest = "output", help = "output file")
  parser.add_option("-i", "--index", action="store", dest = "svr_idx", help = "server index (of current server [0, count-1]")  
  parser.add_option("-c", "--count", action="store", dest = "svr_count", help="number of server in this cluster")  
  (options,args) = parser.parse_args(sys.argv)  
  
  if not options.range_file or not options.svr_idx or not options.svr_count or not options.output:  
    parser.print_help()    
    sys.exit(1)    
        
  svr_count = int(options.svr_count)  
  svr_idx = int(options.svr_idx)  
  tablet_count = 0  
  
  range_input = open(options.range_file, "r")  
  
  for line in range_input:  
    tablet_count += 1    
    
  tablet_count_pern = tablet_count/svr_count  
  
  tablet_beg = tablet_count_pern * svr_idx  
  tablet_end = tablet_beg + tablet_count_pern 
  if svr_idx == svr_count - 1:  
    tablet_end = tablet_count    
  
  range_input.seek(0)  
  
  idx = 0
  output = open(options.output, "w")
  for line in range_input:  
    if idx % svr_count == svr_idx:
      output.write(line)      
    idx += 1
      
      
      
if __name__ == "__main__":
  main()  