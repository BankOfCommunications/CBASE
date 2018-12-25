#!/usr/bin/env python
import os
import sys
import re
import optparse
import textwrap
import shutil
import time
from optparse import OptionParser

class IndentedHelpFormatterWithNL(optparse.IndentedHelpFormatter): 
  def format_description(self, description): 
    if not description: return "" 
    desc_width = self.width - self.current_indent 
    indent = " "*self.current_indent 
    # the above is still the same 
    bits = description.split('\n') 
    formatted_bits = [ 
      textwrap.fill(bit, 
        desc_width, 
        initial_indent=indent, 
        subsequent_indent=indent) 
      for bit in bits] 
    result = "\n".join(formatted_bits) + "\n" 
    return result 
  def format_option(self, option): 
    # The help for each option consists of two parts: 
    #   * the opt strings and metavars 
    #   eg. ("-x", or "-fFILENAME, --file=FILENAME") 
    #   * the user-supplied help string 
    #   eg. ("turn on expert mode", "read data from FILENAME") 
    # 
    # If possible, we write both of these on the same line: 
    #   -x    turn on expert mode 
    # 
    # But if the opt string list is too long, we put the help 
    # string on a second line, indented to the same column it would 
    # start in if it fit on the first line. 
    #   -fFILENAME, --file=FILENAME 
    #       read data from FILENAME 
    self.help_width = 1024
    result = [] 
    opts = self.option_strings[option] 
    opt_width = self.help_position - self.current_indent - 2 
    if len(opts) > opt_width: 
      opts = "%*s%s\n" % (self.current_indent, "", opts) 
      indent_first = self.help_position 
    else: # start help on same line as opts 
      opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts) 
      indent_first = 0 
    result.append(opts) 
    if option.help: 
      help_text = self.expand_default(option) 
      # Everything is the same up through here 
      help_lines = [] 
      for para in help_text.split("\n"): 
        help_lines.extend(textwrap.wrap(para, self.help_width)) 
        # Everything is the same after here 
      result.append("%*s%s\n" % ( 
        indent_first, "", help_lines[0])) 
      result.extend(["%*s%s\n" % (self.help_position, "", line) 
        for line in help_lines[1:]]) 
    elif opts[-1] != "\n": 
      result.append("\n") 
    return "".join(result)

class Config:
  def __init__(self):
    self.conf_dict = {}    
    self.errorno = 0
    
  def load(self, config_fname):  
    config_file = open(config_fname,"r")    
    conf_re = re.compile(r"(?P<key>\S+) *= *(?P<val>\S+)")
    comment_re = re.compile(r"#.*")
    section_re = re.compile(r"\[ *(?P<section>\S+) *\]")
    #cur_section = "default";
    #self.conf_dict[cur_section] = {}
    for line in config_file:    
      line = line.strip()      
      #check if blank line
      if len(line) == 0:
        continue
      
      #check if comment
      mo = comment_re.match(line)      
      if mo:
        continue
      
      #check if section
      mo = section_re.match(line)
      if mo:        
        cur_section = mo.group("section")
        if not self.conf_dict.has_key(cur_section):          
          self.conf_dict[cur_section] = {}            
        continue                
      
      #check if key:val pair      
      mo = conf_re.match(line)      
      if mo:
        self.conf_dict[cur_section][mo.group("key")] = mo.group("val")        
        continue        
      
      print "unrecogonized configuration line %s"%line      
      self.errorno = -1      
    
  def set_val(self, section, key, val):
    self.conf_dict[section][key] = val
    
  def get_int(self, section, key):    
    if not self.conf_dict.has_key(section):      
      return None        
    if not self.conf_dict[section].has_key(key):      
      return None
    return int(self.conf_dict[section][key])      
  
  def get_str(self, section, key):    
    if not self.conf_dict.has_key(section):      
      return None        
    if not self.conf_dict[section].has_key(key):      
      return None
    return self.conf_dict[section][key]      
  
  def errno(self):
    return self.errorno      
  
  def output(self, fname):
    conf_file = open(fname, "w")
    for section in self.conf_dict.keys():
      print>>conf_file, "[%s]"%section
      for key in self.conf_dict[section]:
        print>>conf_file, "%s = %s"%(key, str(self.conf_dict[section][key]))
    conf_file.close()
  
const_olap_config = {
  "cell_count_per_mbyte":1200000,
  "cell_count_per_row":26
  }

def check_options(options, opt_parser):
  #check options
  if not options.conf or not options.action:
    opt_parser.print_help()
    sys.exit(1)
    
  if options.action == "ms_trace_adj" or options.action == "cs_trace_adj" \
     or options.action == "ups_trace_adj" or options.action == "rs_trace_adj":
    if not options.action_option:
      print "action need arguments which not provided [action:%s]"%options.action
      opt_parser.print_help()
      sys.exit(1)
    if "44" != options.action_option and "43" != options.action_option:
      print "action's arguments can only be 44 or 43 [action_option:%s]"%options.action_option
      opt_parser.print_help()
      sys.exit(1)
      
def check_config(config):
  src_uri = config.get_str("deploy", "src_uri")
  #wushi@10.232.35.41:/home/wushi/ms0.3/oceanbase/
  if not src_uri or not re.match("[\w._]+@([\d]+\.){3,3}\d+:(/[\w._]+)+/?", src_uri):
    print "[deploy:src_uri] err %s"%str(src_uri)
    return -1
    
  working_root  =  config.get_str("deploy", "working_root")
  if not working_root or not re.match("(/[\w._]+)*/?[\w+._]", working_root):
    print "[deploy:working_root] err %s"%str(working_root)
    return -1
    
  user_name = config.get_str("deploy", "user_name")
  if not user_name or not re.match("[\w._]+", user_name):
    print "[deploy:user_name] err %s"%str(user_name)
    return -1
   
  ups_rs = config.get_str("deploy", "ups_rs")
  if not ups_rs or not re.match("(\d+\.){3,3}\d+", ups_rs):
    print "[deploy:ups_rs] err %s"%ups_rs
    return -1
  
  rs_port = config.get_int("deploy", "rs_port")
  if not rs_port or rs_port <= 0 or rs_port >= 65535:
    print "[deploy:rs_port] err %s"%(str(rs_port))
    return -1
  
  ups_port = config.get_int("deploy", "ups_port")
  if not ups_port or ups_port <= 0 or ups_port >= 65535:
    print "[deploy:ups_port] err %s"%(str(ups_port))
    return -1
  
  ups_inner_port = config.get_int("deploy", "ups_inner_port")
  if not ups_inner_port or ups_inner_port <= 0 or ups_inner_port >= 65535:
    print "[deploy:ups_inner_port] err %s"%(str(ups_inner_port))
    return -1
  
  cs_port = config.get_int("deploy", "cs_port")
  if not cs_port or cs_port <= 0 or cs_port >= 65535:
    print "[deploy:cs_port] err %s"%(str(cs_port))
    return -1


  application_name = config.get_str("deploy", "application_name")
  if not application_name or not re.match("[\w._]+", application_name):
    print "[deploy:application_name] err %s"%str(application_name)
    return -1
  
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  if not tablet_size_mbyte or tablet_size_mbyte < 8 or tablet_size_mbyte > 300:
    print "[deploy:tablet_size_mbyte] err %s"%str(tablet_size_mbyte)
    return -1
  
  total_tablet_count = config.get_int("deploy", "total_tablet_count")
  if not total_tablet_count or total_tablet_count < 10 or total_tablet_count >= 300:
    print "[deploy:total_tablet_count] err %s"%str(total_tablet_count)
    return -1

  
  start_key = config.get_int("olap", "start_key")
  if not start_key or start_key < 0 or start_key > 2**32:
    print "[olap:start_key] err %s"%str(start_key)
    return -1
  
  end_key = config.get_int("olap", "end_key")
  if not end_key or end_key < 0 or end_key > 2**32 or end_key <= start_key:
    print "[olap:end_key] err %s"%str(end_key)
    return -1
  
  need_row_count  = total_tablet_count*tablet_size_mbyte*const_olap_config["cell_count_per_mbyte"]/const_olap_config["cell_count_per_row"]
  if end_key - start_key + 1 < need_row_count:
    print "[olap] row count too small [start_key:%d,end_key:%d,need_row_count:%d]"%(start_key,end_key,need_row_count)
    return -1
     
  
  scan_count_min = config.get_int("olap", "scan_count_min")
  if not scan_count_min or scan_count_min <= 0 or scan_count_min > need_row_count:
    print "[olap:scan_count_min] err %s"%str(scan_count_min)
    return -1
  
  scan_count_max = config.get_int("olap", "scan_count_max")
  if not scan_count_max or scan_count_max <= scan_count_min:
    print "[olap:scan_count_max] err %s"%str(scan_count_max)
    return -1
  
  network_timeout_us = config.get_int("olap", "network_timeout_us")
  if not network_timeout_us or network_timeout_us <= 0:
    print "[olap:network_timeout_us] err %s"%str(network_timeout_us)
    return -1
  
  read_thread_count = config.get_int("olap", "read_thread_count")
  if not read_thread_count or read_thread_count <= 0 or read_thread_count >= 100:
    print "[olap:read_thread_count] err %s"%str(read_thread_count)
    return -1
  
  ms_list_re = re.compile("(((?:\d+\.){3,3}\d+):(\d+),?)+")
  ms_list = config.get_str("olap", "ms_list")
  if not ms_list or not ms_list_re.match(ms_list):
    print "[olap:ms_list] err %s"%str(ms_list)
    return -1
  ms_list_re = re.compile("((?:\d+\.){3,3}\d+):(\d+),?")
  mss = ms_list_re.findall(ms_list)
  ms_port = mss[0][1]
  for idx in xrange(1,len(mss)):
    if mss[idx][1] != ms_port:
      print "mergeserver ports are not all the same"
      return -1
  
  log_level = config.get_str("olap", "log_level")
  if not log_level or (log_level != "debug" and log_level != "info" 
                       and log_level != "warn" and log_level != "error"):
    print "[olap:log_level] err %s"%str(log_level)
    return -1
  
  log_path = config.get_str("olap", "log_path")
  if not log_path or not re.match("([\w._]+)?(/[\w._]+)*/?[\w._]+",log_path):
    print "[olap:log_path] err %s"%str(log_path)
    return -1
  
  return 0

def gen_confs(config):
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  ups_port = config.get_int("deploy", "ups_port")
  ups_inner_port = config.get_int("deploy", "ups_inner_port")
  cs_port = config.get_int("deploy", "cs_port")
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  application_name = config.get_str("deploy", "application_name")
  
  ms_conf = Config()
  ms_conf.load("./mergeserver.conf.template")
  if 0 != ms_conf.errno():
    print "fail to load mergeserver configuration"
    return -1
  ms_conf.set_val("root_server", "vip",ups_rs)
  ms_conf.set_val("root_server", "port", rs_port)
  ms_conf.set_val("merge_server", "port", ms_port)
  ms_conf.output("mergeserver.conf")
  
  cs_conf = Config()
  cs_conf.load("./chunkserver.conf.template")
  if 0 != cs_conf.errno():
    return -1
  cs_conf.set_val("root_server", "vip",ups_rs)
  cs_conf.set_val("root_server", "port", rs_port)
  cs_conf.set_val("chunk_server", "port", cs_port)
  cs_conf.set_val("chunk_server", "application_name", application_name)
  cs_conf.output("chunkserver.conf")
  
  ups_conf = Config()
  ups_conf.load("./updateserver.conf.template")
  if 0 != ups_conf.errno():
    return -1
  ups_conf.set_val("update_server", "vip", ups_rs)
  ups_conf.set_val("update_server", "port", ups_port)
  ups_conf.set_val("update_server", "ups_inner_port", ups_inner_port)
  ups_conf.set_val("root_server", "vip",ups_rs)
  ups_conf.set_val("root_server", "port", rs_port)
  ups_conf.output("updateserver.conf")
  
  rs_conf = Config()
  rs_conf.load("./rootserver.conf.template")
  if 0 != rs_conf.errno():
    return -1
  rs_conf.set_val("ob_instances", "obi0_rs_vip", ups_rs);
  rs_conf.set_val("ob_instances", "obi0_rs_port", ups_port);
  rs_conf.set_val("root_server", "vip",ups_rs)
  rs_conf.set_val("root_server", "port", rs_port)
  rs_conf.set_val("update_server", "vip", ups_rs)
  rs_conf.set_val("update_server", "port", ups_port)
  rs_conf.set_val("update_server", "ups_inner_port", ups_inner_port)
  rs_conf.set_val("update_server", "ups0_ip", ups_rs);
  rs_conf.set_val("update_server", "ups0_port", ups_port);
  rs_conf.set_val("update_server", "ups0_inner_port", ups_inner_port);
  rs_conf.output("rootserver.conf")
  
  return 0


def get_ms_list_port(ms_list):
  ms_list_re = re.compile("((?:\d+\.){3,3}\d+):(\d+),?")
  mss = ms_list_re.findall(ms_list)
  ms_port = mss[0][1]
  ms_ips = []
  for i in xrange(0,len(mss)):
    ms_ips.append(mss[i][0])
  return (ms_ips,ms_port)

def update_cs(config):
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)

  
  for ms in ms_list:
    cmd = "scp %s/src/chunkserver/chunkserver %s@%s:%s/chunkserver/"%(src_uri,user_name,ms, working_root) 
    if 0 != os.system(cmd):
      print "fail to update cs [cmd:%s]"%cmd
      return -1
    
    cmd = "scp chunkserver.conf %s@%s:%s/chunkserver"%(user_name, ms, working_root) 
    if 0 != os.system(cmd):
      print "fail to copy chunkserver.conf [cmd:%s]"%cmd
      return -1

    cmd = r'ssh %s@%s "rm -rf %s/chunkserver/chunkserver.log*"'%(user_name,ms,working_root)
    if 0 != os.system(cmd):
      print "fail to delete chunkserver.log [cmd:%s]"%cmd
      return -1 
    
  return 0
      

def update_ms(config):
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)

  
  for ms in ms_list:
    cmd = "scp %s/src/mergeserver/mergeserver %s@%s:%s/mergeserver"%(src_uri,user_name, ms, working_root) 
    if 0 != os.system(cmd):
      print "fail to update cs [cmd:%s]"%cmd
      return -1
    
    cmd = "scp mergeserver.conf %s@%s:%s/mergeserver"%(user_name, ms, working_root) 
    if 0 != os.system(cmd):
      print "fail to copy mergeserver.conf [cmd:%s]"%cmd
      return -1

    cmd = r'ssh %s@%s "rm -rf %s/mergeserver/mergeserver.log*"'%(user_name,ms,working_root)
    if 0 != os.system(cmd):
      print "fail to delete mergeserver.log [cmd:%s]"%cmd
      return -1 

  return 0


def update_ups(config):
  ups_rs = config.get_str("deploy", "ups_rs")
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)

  
  cmd = "scp %s/src/updateserver/updateserver %s@%s:%s/updateserver"%(src_uri,user_name, ups_rs, working_root) 
  if 0 != os.system(cmd):
    print "fail to update ups [cmd:%s]"%cmd
    return -1

  cmd = "scp updateserver.conf %s@%s:%s/updateserver"%(user_name, ups_rs, working_root) 
  if 0 != os.system(cmd):
    print "fail to copy updateserver.conf [cmd:%s]"%cmd
    return -1
  
  return 0


def update_rs(config):
  ups_rs = config.get_str("deploy", "ups_rs")
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)

  
  cmd = "scp %s/src/rootserver/rootserver %s@%s:%s/rootserver"%(src_uri,user_name, ups_rs, working_root) 
  if 0 != os.system(cmd):
    print "fail to update rs [cmd:%s]"%cmd
    return -1
  
  cmd = "scp schema.ini rootserver.conf %s@%s:%s/rootserver"%(user_name, ups_rs, working_root) 
  if 0 != os.system(cmd):
    print "fail to copy rootserver.conf [cmd:%s]"%cmd
    return -1
  
  return 0

def update_tools(config):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  if os.path.exists("./tools"):
    shutil.rmtree("./tools")
    
  os.mkdir("./tools")
  os.mkdir("./tools/libs")
  cmd = "scp %s/tools/gen_meta %s/tests/mergeserver/olap/gen_range %s/tests/mergeserver/olap/gen_ups_sst "\
      "%s/src/rootserver/rs_admin %s/tools/ups_admin ./tools/"%(src_uri, src_uri, src_uri, src_uri, src_uri)
  if 0 != os.system(cmd):
    print "fail to generate tools needed by deploy"
    return -1
  
  cmd = "scp %s/src/common/compress/.libs/* ./tools/libs/"%(src_uri)
  if 0 != os.system(cmd):
    print "fail to copy compress libs [cmd:%s]"%cmd
    return -1
  shutil.copy("./distribute_range.py", "tools")
  return 0

  


def deploy(config):
  if 0 != gen_confs(config):
    print "fail to generate configurations"
    return -1
  
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  if not os.path.exists("./tools"):
    if 0 != update_tools(config):
      return -1

  ms_idx = 0 
  ms_count = len(ms_list)
  for ms in ms_list:
    #create working root
    cmd = r'ssh %s@%s "rm -rf %s"'%(user_name,ms,working_root)
    if 0 != os.system(cmd):
      print "fail to rm top working dire [cmd:%s]"%cmd
      return -1
    
    cmd = r'ssh %s@%s "mkdir -p %s"'%(user_name, ms, working_root)
    if 0 != os.system(cmd):
      print "fail to create top working dir [cmd:%s]"%cmd
      return -1
    
    cmd = r'ssh %s@%s "cd %s && mkdir chunkserver mergeserver"'%(user_name, ms, working_root)
    if 0 != os.system(cmd):
      print "fail to create exec dirs [cmd:%s]"%cmd
      return -1
    
    cmd = r'ssh %s@%s "for (( i = 1; i <= 10; i++)); do rm -rf /data/\$i/%s/*;done"'%(user_name, ms, application_name)
    if 0 != os.system(cmd):
      print "fail to delete /data/[i]/%s/sstable [cmd:%s]"%(application_name,cmd)
      return -1
    
    cmd = r'ssh %s@%s "for (( i = 1; i <= 10; i++)); do mkdir -p /data/\$i/%s/sstable;done"'%(user_name, ms, application_name)
    if 0 != os.system(cmd):
      print "fail to create /data/[i]/%s/sstable [cmd:%s]"%(application_name,cmd)
      return -1
    
    cmd = "scp -r tools %s@%s:%s/"%(user_name,ms,working_root)
    if 0 != os.system(cmd):
      print "fail to deploy tools [cmd:%s]"%cmd
      return -1
    
    print "generate sstable index"
    start_key = config.get_int("olap", "start_key")
    end_key = config.get_int("olap", "end_key")
    tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")

    cmd = r'ssh %s@%s "cd %s/tools/ &&  ./gen_range -s %d -e %d -i %d > all_ranges.txt 2>&1 && ./distribute_range.py -f all_ranges.txt -i %d -c %d -o ranges.txt && echo \"1,1101,(MIN,MAX]\" >> ranges.txt && echo \"1,1102,(MIN,MAX]\" >> ranges.txt  && ./gen_meta -r ranges.txt -v 1 -x 1 >/dev/null 2>&1;  "'%(user_name, ms, working_root, start_key, end_key, tablet_size_mbyte*const_olap_config["cell_count_per_mbyte"]/const_olap_config["cell_count_per_row"], ms_idx, ms_count)
    if 0 != os.system(cmd):
      print "fail to create tablet ranges [cmd:%s]"%cmd
      return -1
    
    cmd = r'ssh %s@%s "cd %s/tools/ &&  for i in {1..10}; do rm -rf /data/\$i/%s/sstable/*; mv idx_1_\$i /data/\$i/%s/sstable/; done"'%(user_name, ms, working_root, application_name, application_name)
    if 0 != os.system(cmd):
      print "fail to copy tablet ranges [cmd:%s]"%cmd
      return -1
    
    print "cs and ms deployed on %s ------------------"%ms
    ms_idx += 1

  if 0 != update_cs(config):
    return -1
  if 0 != update_ms(config):
    return -1
    
  cmd = r'ssh %s@%s "rm -rf %s/updateserver %s/rootserver && mkdir -p %s/updateserver/commitlog %s/rootserver/commitlog &&  mkdir -p %s/updateserver/data/raid1 && mkdir -p /data/1/%s/us && ln -sf /data/1/%s/us %s/updateserver/data/raid1/store1"'%(user_name,ups_rs,working_root,working_root,working_root,working_root,working_root,application_name,application_name,working_root)
  if 0 != os.system(cmd):
    print "fail to create directory for ups [cmd:%s]"%cmd
    return -1
  
  if 0 != update_ups(config):
    return -1
    
  if 0 != update_rs(config):
    return -1
  
  print "ups & rs deployed -------"
  
  if ms_list.count(ups_rs) == 0:
    cmd = "scp -r tools %s@%s:%s/"%(user_name,ups_rs,working_root)
    if 0 != os.system(cmd):
      print "fail to deploy tools [cmd:%s]"%cmd
      return -1
    
  print "begin to generate sst for ups ..."
  cmd = r'ssh %s@%s "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:%s/tools/libs; cd %s/tools && rm -rf ../updateserver/data/raid1/store1/* && cat ../rootserver/schema.ini |grep -v column_group_info > gen_schema.ini  && ./gen_ups_sst -s %d -e %d -p ../updateserver/data/raid1/store1/2_1-1_10.sst -m gen_schema.ini"'%(user_name, ups_rs, working_root, working_root, start_key, end_key)
  if 0 != os.system(cmd):
    print "fail to generate sst for ups [cmd:%s]"%cmd
    return -1
  return 0

def start_ups(config):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  cmd = r'ssh %s@%s "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:%s/tools/libs;cd %s/updateserver && ./updateserver"'%(user_name,ups_rs,working_root, working_root)
  if 0 != os.system(cmd):
    print "fail to start updateserver [cmd:%s]"%cmd
    return -1
  
  return 0


def kill_ups(config, signal = None):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  if not signal:
    cmd = r'ssh %s@%s "kill \`cat %s/updateserver/updateserver.pid\`"'%(user_name,ups_rs,working_root)
  else:
    cmd = r'ssh %s@%s "kill -%d \`cat %s/updateserver/updateserver.pid\`"'%(user_name,ups_rs,signal, working_root)
  if 0 != os.system(cmd):
    print "fail to kill updateserver [cmd:%s]"%cmd
    return -1
  
  return 0


def start_rs(config):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  cmd = r'ssh %s@%s "cd %s/rootserver && ./rootserver"'%(user_name,ups_rs,working_root)
  if 0 != os.system(cmd):
    print "fail to start rootserver [cmd:%s]"%cmd
    return -1
  
  return 0


def kill_rs(config, signal = None):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  if not signal:
    cmd = r'ssh %s@%s "kill \`cat %s/rootserver/rootserver.pid\`"'%(user_name,ups_rs,working_root)
  else:
    cmd = r'ssh %s@%s "kill -%d \`cat %s/rootserver/rootserver.pid\`"'%(user_name,ups_rs,signal, working_root)
  if 0 != os.system(cmd):
    print "fail to kill rootserver [cmd:%s]"%cmd
    return -1
  
  return 0


def set_obi_role(config, role):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  cmd = r'ssh %s@%s "cd %s/tools && ./rs_admin -r 127.0.0.1 -p %d set_obi_role -o %s"'%(user_name,ups_rs,working_root, rs_port, role)
  if 0 != os.system(cmd):
    print "fail to set obi role [cmd:%s]"%cmd
    return -1
  
  return 0


def start_cs(config):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  for ms in ms_list:
    cmd = r'ssh %s@%s "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:%s/tools/libs; cd %s/chunkserver && ./chunkserver"'%(user_name,ms,working_root, working_root)
    if 0 != os.system(cmd):
      print "fail to start chunkserver [cmd:%s]"%cmd
      return -1
  
  return 0


def kill_cs(config, signal = None):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  for ms in ms_list:
    if not signal:
      cmd = r'ssh %s@%s "kill \`cat %s/chunkserver/chunkserver.pid\`"'%(user_name,ms,working_root)
    else :
      cmd = r'ssh %s@%s "kill -%d \`cat %s/chunkserver/chunkserver.pid\`"'%(user_name,ms,signal, working_root)
    if 0 != os.system(cmd):
      print "fail to kill chunkserver [cmd:%s]"%cmd
      return -1
  
  return 0


def start_ms(config):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  for ms in ms_list:
    cmd = r'ssh %s@%s "cd %s/mergeserver && ./mergeserver"'%(user_name,ms,working_root)
    if 0 != os.system(cmd):
      print "fail to start mergeserver [cmd:%s]"%cmd
      return -1
  
  return 0


def kill_ms(config, signal = None):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  for ms in ms_list:
    if not signal:
      cmd = r'ssh %s@%s "kill \`cat %s/mergeserver/mergeserver.pid\`"'%(user_name,ms,working_root)
    else :
      cmd = r'ssh %s@%s "kill -%d \`cat %s/mergeserver/mergeserver.pid\`"'%(user_name,ms,signal, working_root)
    if 0 != os.system(cmd):
      print "fail to kill mergeserver [cmd:%s]"%cmd
      return -1
  
  return 0


def start(config):
  if 0 != start_rs(config):
    return -1
  time.sleep(1)
  
  if 0 != start_ups(config):
    return -1
  
  if 0 != start_cs(config):
    return -1
  
  if 0 != start_ms(config):
    return -1
  
  if 0 != set_obi_role(config,"OBI_MASTER"):
    return -1
  
  return 0


def stop(config):
  if 0!= kill_rs(config):
    print "fail to kill cs"
  if 0 != kill_ups(config):
    print "fail to kill ups"
  if 0 != kill_cs(config):
    print "fail to kill cs"
  if 0 != kill_ms(config):
    print "fail to kill ms"
  return 0


def clean(config):
  src_uri = config.get_str("deploy", "src_uri")
  working_root  =  config.get_str("deploy", "working_root")
  user_name = config.get_str("deploy", "user_name")
  if working_root[-1] != "/":
    working_root = working_root + "/"
  if working_root[0] != "/":
    working_root = "/home/%s/%s"%(user_name, working_root)
  (ms_list,ms_port) = get_ms_list_port(config.get_str("olap", "ms_list"))
  ups_rs = config.get_str("deploy", "ups_rs")
  rs_port = config.get_int("deploy", "rs_port")
  application_name = config.get_str("deploy", "application_name")
  tablet_size_mbyte = config.get_int("deploy", "tablet_size_mbyte")
  start_key = config.get_int("olap", "start_key")
  end_key = config.get_int("olap", "end_key")
  scan_count_min = config.get_int("olap", "scan_count_min")
  scan_count_max = config.get_int("olap", "scan_count_max")
  
  stop(config)
  
  ms_list.append(ups_rs)
  for ms in ms_list:
    cmd = r'ssh %s@%s "rm -rf %s && for ((i = 1; i <= 10; i++ )); do rm -rf /data/\$i/%s;done"'%(user_name,
                                                                                                 ms, working_root,
                                                                                                 application_name)
    if os.system(cmd) != 0:
      print "fail to clean environment on %s"%ms
  return 0

  
    
def main():
  action_help = \
  """action to perform\n
  deploy\t: set up a new empty environment according to configuration file\n
  start\t\t: start all servers\n
  stop\t\t: stop all servers\n
  clean\t\t: stop and clean the while environment\n
  stop_cs\t: stop all chunkservers\n
  start_cs\t: start all chunkservers\n
  update_cs\t: update all chunkservers exc\n
  stop_ms\t: stop all mergeservers\n
  start_ms\t: start all mergeservers\n
  update_ms\t: update all mergeservers exc\n
  stop_ups\t: stop all updateserver\n
  start_ups\t: start all updateserver\n
  update_ups\t: update all updateserver exc\n
  stop_rs\t: stop all rootserver\n
  start_rs\t: start all rootserver\n
  update_rs\t: update all rootserver exc\n
  ms_trace_adj\t: adjust mergeserver trace log level [-o [44,43]]\n
  cs_trace_adj\t: adjust chunkserver trace log level [-o [44,43]]\n
  ups_trace_adj\t: adjust updateserver trace log level [-o [44,43]]\n
  rs_trace_adj\t: adjust rootserver trace log level [-o [44,43]]\n
  major_freeze\t: let ups do major freeze\n
  trig_master\t: set obi role of rootserver to master\n
  trig_slave\t: set obi role of rootserver to slave\n
  gen_confs\t: generate configurations of servers\n
  check_config\t: check correction of configuration file \n
  update_tools\t: update local copy of tools used
  """
  parser = OptionParser(formatter=IndentedHelpFormatterWithNL())
  parser.add_option("-f", "--file", action="store", dest="conf", help="configuation file path")    
  parser.add_option("-a", "--action", action="store", dest="action", 
                    help= action_help,
                    choices = ["deploy","start", "stop", "clean", 
                               "stop_cs","start_cs","update_cs",  
                               "stop_ms","start_ms","update_ms", 
                               "stop_ups", "start_ups", "update_ups",
                               "stop_rs", "start_rs", "update_rs",
                               "ms_trace_adj", "cs_trace_adj", "ups_trace_adj", "rs_trace_adj",
                               "major_freeze", "trig_master", "trig_slave", "gen_confs",
                               "check_config", "update_tools"]
                    )
  parser.add_option("-o","--action-option", action = "store", dest="action_option", 
                    help = "arguments of action, not all actions need arguments")
  (options, args) = parser.parse_args(sys.argv)  
  
  check_options(options, parser)
    
  conf = Config()
  conf.load(options.conf)  
  if 0 != conf.errno():
    print "fail to load configuration"
    sys.exit(1)    
    
  if 0 != check_config(conf):
    print "check config pass"
    sys.exit(1)
    
  if "deploy" == options.action:
    if 0 != deploy(conf):
      print "fail to deploy oceanbase"
    else:
      print "deploy success"
  elif "gen_confs" == options.action:
    if 0 != gen_confs(conf):
      print "fail to generate configurations for servers"
    else:
      print "configuration files generation successful"
  elif "check_config" == options.action:
    if 0 != check_config(conf):
      print "fail to check configuration"
    else:
      print "configuration checking pass"
  elif "start" == options.action:
    if 0 != start(conf):
      print "fail to start service"
    else:
      print "start service success"
  elif "stop" == options.action:
    if 0 != stop(conf):
      print "fail to stop service"
    else :
      print "stop service successful"
  elif "trig_master" == options.action:
    if 0 != set_obi_role(conf,"OBI_MASTER"):
      print "fail to set master"
    else :
      print "successfull to set master"
  elif "trig_slave" == options.action:
    if 0 != set_obi_role(conf,"OBI_SLAVE"):
      print "fail to set slave"
    else :
      print "successfull to set slave"

  elif "update_ups" == options.action:
    if 0 != update_ups(conf):
      print "fail to update ups"
    else : 
      print "update ups success"
  elif "update_rs" == options.action:
    if 0 != update_rs(conf):
      print "fail to update rs"
    else :
      print "update rs success"
  elif "update_cs" == options.action:
    if 0 != update_cs(conf):
      print "fail to update cs"
    else :
      print "update cs success"
  elif "update_ms" == options.action:
    if 0 != update_ms(conf):
      print "fail to update ms"
    else :
      print "update ms success"
  elif "start_rs" == options.action:
    if 0 != start_rs(conf):
      print "fail to start rs"
    else:
      print "start rs successful"
  elif "start_ups" == options.action:
    if 0 != start_ups(conf):
      print "fail to start ups"
    else:
      print "start ups successful"
  elif "start_cs" == options.action:
    if 0 != start_cs(conf):
      print "fail to start cs"
    else:
      print "start cs successful"
  elif "start_ms" == options.action:
    if 0 != start_ms(conf):
      print "fail to start ms"
    else:
      print "start ms successful"
  elif "stop_rs" == options.action:
    if 0 != kill_rs(conf):
      print "fail to stop rs"
    else:
      print "stop rs successful"
  elif "stop_ups" == options.action:
    if 0 != kill_ups(conf):
      print "fail to stop ups"
    else:
      print "stop ups successful"
  elif "stop_cs" == options.action:
    if 0 != kill_cs(conf):
      print "fail to stop cs"
    else:
      print "stop cs successful"
  elif "stop_ms" == options.action:
    if 0 != kill_ms(conf):
      print "fail to stop ms"
    else:
      print "stop ms successful"
  elif "rs_trace_adj" == options.action:
    if 0 != kill_rs(conf, int(options.action_option)):
      print "fail to adjust rs trace log"
    else:
      print "success to adjust rs trace log"
  elif "ups_trace_adj" == options.action:
    if 0 != kill_ups(conf, int(options.action_option)):
      print "fail to adjust ups trace log"
    else:
      print "success to adjust ups trace log"
  elif "cs_trace_adj" == options.action:
    if 0 != kill_cs(conf, int(options.action_option)):
      print "fail to adjust cs trace log"
    else:
      print "success to adjust cs trace log"
  elif "ms_trace_adj" == options.action:
    if 0 != kill_ms(conf, int(options.action_option)):
      print "fail to adjust ms trace log"
    else:
      print "success to adjust ms trace log"      
  elif "clean" == options.action:
    if 0 != clean(conf):
      print "fail to clean environment"
    else :    
      print "clean environment successful"      
  elif "update_tools" == options.action:
    if 0 != update_tools(conf):
      print "fail to update local copy of tools"
    else:
      print "update local copy of tools successful"
      
  return 0  

if __name__ == "__main__":
  main()
