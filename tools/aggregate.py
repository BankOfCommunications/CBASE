import os
import paramiko
import subprocess
import re
import collections
import sqlite3
import types

list = ""
connection = ""
cursor = ""

def connect_db():
  global connection
  connection = sqlite3.connect("./profile.db")
  global cursor
  cursor = connection.cursor()
  cursor.execute("CREATE TABLE MS_PROFILE(trace_id bigint,source_chid int,chid int,ms_sql varchar(256), ms_resolve_sql_time_us bigint, ms_scan_root_table_start_time bigint, ms_scan_root_table_end_time bigint, ms_handle_sql_time_ms bigint, ms_pcode int,ms_sync_rpc_start_time bigint default(0),ms_sync_rpc_end_time bigint default(0),ms_self varchar(32),ms_peer varchar(32),ms_ret int,ms_async_rpc_start_time bigint default(0),ms_async_rpc_end_time bigint default(0), ms_wait_time_us_in_sql_queue bigint, ms_wait_time_us_in_queue bigint, ms_handle_packet_start_time bigint, ms_handle_packet_end_time bigint, ms_packet_received_time bigint, primary key(trace_id,chid))")
  cursor.execute("CREATE TABLE CS_PROFILE(trace_id bigint,source_chid int,chid int, cs_pcode int,cs_sync_rpc_start_time bigint default(0),cs_sync_rpc_end_time bigint default(0),cs_self varchar(32),cs_peer varchar(32),cs_ret int,cs_async_rpc_start_time bigint default(0),cs_async_rpc_end_time bigint default(0),cs_wait_time_us_in_queue bigint, cs_handle_packet_start_time bigint, cs_handle_packet_end_time bigint, cs_packet_received_time bigint, primary key(trace_id,source_chid))")
  cursor.execute("CREATE TABLE UPS_PROFILE(trace_id bigint,source_chid int,chid int, ups_pcode int,ups_sync_rpc_start_time bigint default(0),ups_sync_rpc_end_time bigint default(0),ups_self varchar(32),ups_peer varchar(32),ups_ret int,ups_async_rpc_start_time bigint default(0),ups_async_rpc_end_time bigint default(0),ups_wait_time_us_in_queue bigint, ups_wait_time_us_in_write_queue bigint, ups_wait_time_us_in_commit_queue bigint, ups_wait_time_us_in_rw_queue bigint, ups_handle_packet_start_time bigint, ups_handle_packet_end_time bigint, ups_packet_received_time bigint, ups_req_start_time bigint, ups_req_end_time bigint, ups_scanner_size_bytes bigint, primary key(trace_id,source_chid))")
  cursor.execute("CREATE TABLE RS_PROFILE(trace_id bigint,source_chid int,chid int, rs_pcode int,rs_sync_rpc_start_time bigint default(0),rs_sync_rpc_end_time bigint default(0),rs_self varchar(32),rs_peer varchar(32),rs_ret int,rs_async_rpc_start_time bigint default(0),rs_async_rpc_end_time bigint default(0),rs_wait_time_us_in_queue bigint,rs_handle_packet_start_time bigint, rs_handle_packet_end_time bigint, rs_packet_received_time bigint,primary key(trace_id,source_chid))")
def download_files():
  conf = open("aggregate.conf", "r");
  for line in conf:
    if line.startswith('#'):
      continue
    pair = line.split("=")
    if pair[0] == "username":
      username = pair[1].strip()
    elif pair[0] == "ip":
      ip = pair[1].strip()
      global list
      list = ip.split(",")
    elif pair[0] == "logdir":
      logdir = pair[1].strip()
  ssh = paramiko.SSHClient()
  known_hosts = "/home/"
  known_hosts += username
  known_hosts += "/.ssh/known_hosts"
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh.load_system_host_keys(known_hosts)
  for host in list:
    os.mkdir(host)
    ssh.connect(host, username=username)
    command = "ls "
    command += logdir
    command += " | grep profile"
    stdin,stdout,stderr = ssh.exec_command(command)
    scp = "scp "
    scp += username
    scp += "@"
    scp += host
    scp += ":"
    scp += logdir
    scp += "/"
    while True:
      file = stdout.readline()
      if file:
        scp = "scp "
        scp += username
        scp += "@"
        scp += host
        scp += ":"
        scp += logdir
        scp += "/"
        scp += file.strip()
        scp += " "
        scp += host
        os.system(scp)
      else:
        break
  conf.close()

#handle mergeserver profile log
def aggregate_mergeserver():
  global cursor
  global connection
  for dir in list:
    mergeserver = "ls %s | grep merge" % dir
    data = subprocess.Popen(mergeserver, stdout=subprocess.PIPE, shell=True)
    while True:
      ms = data.stdout.readline().strip()
      if ms:
        path = "%s/%s" % (dir, ms)
        ms_fd = open(path, "r")
        for line in ms_fd:
          line = line.strip()
          org_list = re.findall('(\w+)=\[(.*?)\]', line)
          map = dict(org_list)
          #remove repetition
          '''
          trace_list = re.findall('trace_id=\[(\d+)\]',line)
          no_repeat_list = org_list
          if len(trace_list) == 2:
            for index in range(len(no_repeat_list)):
              if (no_repeat_list[index])[0] == "trace_id":
                no_repeat_list.remove(no_repeat_list[index])
                break
            for index in range(len(no_repeat_list)):
              if (no_repeat_list[index])[0] == "chid":
                no_repeat_list.remove(no_repeat_list[index])
                break
                '''
          check_whether_exist_sql = "select * from MS_PROFILE where trace_id= %s and chid=%s" % (map["trace_id"], map["chid"])
          cursor.execute(check_whether_exist_sql)
          res = cursor.fetchone()
          if type(res) is types.NoneType:
            insert_sql = "insert into ms_profile(trace_id, source_chid, chid) values(%s, %s, %s)" % (map["trace_id"], map["source_chid"], map["chid"])
            cursor.execute(insert_sql)
            connection.commit()
          update_sql = "update ms_profile set "
          flag = False
          for key in map:
            if key == "trace_id" or key == "source_chid" or key == "chid":
              continue
            else:
              if flag == False:
                if key == "self" or key == "peer":
                  update_sql += " ms_%s = '%s'" % (key, map[key])
                elif key == "sql":
                  update_sql += " ms_%s = '%s'" % (key, map[key].replace("'", r"''"))
                else:
                  update_sql += " ms_%s = %s" % (key, map[key])
                flag = True
              else:
                if key == "self" or key == "peer":
                  update_sql += ", ms_%s = '%s'" % (key, map[key])
                elif key == "sql":
                  update_sql += ", ms_%s = '%s'" % (key, map[key].replace("'", r"''"))
                else:
                  update_sql += ",  ms_%s = %s" % (key, map[key])
          #update_sql += " where trace_id = %s and source_chid = %s and chid = %s" % (map["trace_id"], map["source_chid"], map["chid"])
          update_sql += " where trace_id = %s and chid = %s" % (map["trace_id"], map["chid"])
          cursor.execute(update_sql)
          connection.commit()
      else:
        break
#handle chunkserver
def aggregate_chunkserver():
  global connection
  global cursor
  for dir in list:
    chunkserver = "ls %s | grep chunk" % dir
    data = subprocess.Popen(chunkserver, stdout=subprocess.PIPE, shell=True)
    while True:
      cs = data.stdout.readline().strip()
      if cs:
        path = dir
        path += "/"
        path += cs
        cs_fd = open(path, "r")
        for line in cs_fd:
          line = line.strip()
          org_list = re.findall('(\w+)=\[(.*?)\]', line)
          map = dict(org_list)
          check_whether_exist_sql = "select * from CS_PROFILE where trace_id= %s and source_chid=%s" % (map["trace_id"], map["source_chid"])
          cursor.execute(check_whether_exist_sql)
          res = cursor.fetchone()
          if type(res) is types.NoneType:
            insert_sql = "insert into cs_profile(trace_id, source_chid, chid) values(%s, %s, %s)" % (map["trace_id"], map["source_chid"], map["chid"])
            cursor.execute(insert_sql)
            connection.commit()
          update_sql = "update cs_profile set "
          flag = False
          for key in map:
            if key == "trace_id" or key == "source_chid":
              continue
            else:
              if flag == False:
                if key == "self" or key == "peer":
                  update_sql += " cs_%s = '%s'" % (key, map[key])
                elif key == "chid":
                  update_sql += " %s = %s" % (key, map[key])
                else:
                  update_sql += " cs_%s = %s" % (key, map[key])
                flag = True
              else:
                if key == "self" or key == "peer":
                  update_sql += ", cs_%s = '%s'" % (key, map[key])
                elif key == "chid":
                  update_sql += ", %s = %s" % (key, map[key])
                else:
                  update_sql += ",  cs_%s = %s" % (key, map[key])
          update_sql += " where trace_id = %s and source_chid = %s" % (map["trace_id"], map["source_chid"])
          cursor.execute(update_sql)
          connection.commit()
      else:
        break
def aggregate_updateserver():
  global connection
  global cursor
  for dir in list:
    updateserver = "ls %s | grep update" % dir
    data = subprocess.Popen(updateserver, stdout=subprocess.PIPE, shell=True)
    while True:
      ups = data.stdout.readline().strip()
      if ups:
        path = dir
        path += "/"
        path += ups
        ups_fd = open(path, "r")
        for line in ups_fd:
          line = line.strip()
          org_list = re.findall('(\w+)=\[(.*?)\]', line)
          map = dict(org_list)
          check_whether_exist_sql = "select * from UPS_PROFILE where trace_id= %s and source_chid=%s" % (map["trace_id"], map["source_chid"])
          cursor.execute(check_whether_exist_sql)
          res = cursor.fetchone()
          if type(res) is types.NoneType:
            insert_sql = "insert into UPS_PROFILE(trace_id, source_chid) values(%s, %s)" % (map["trace_id"], map["source_chid"])
            cursor.execute(insert_sql)
            connection.commit()
          update_sql = "update ups_profile set "
          flag = False
          for key in map:
            if key == "trace_id" or key == "source_chid" or key == "chid":
              continue
            else:
              if flag == False:
                if key == "self" or key == "peer":
                  update_sql += " ups_%s = '%s'" % (key, map[key])
                else:
                  update_sql += " ups_%s = %s" % (key, map[key])
                flag = True
              else:
                if key == "self" or key == "peer":
                  update_sql += ", ups_%s = '%s'" % (key, map[key])
                else:
                  update_sql += ",  ups_%s = %s" % (key, map[key])
          update_sql += " where trace_id = %s and source_chid = %s" % (map["trace_id"], map["source_chid"])
          cursor.execute(update_sql)
          connection.commit()
      else:
        break
def aggregate_rootserver():
  global connection
  global cursor
  for dir in list:
    rootserver = "ls %s | grep root" % dir
    data = subprocess.Popen(rootserver, stdout=subprocess.PIPE, shell=True)
    while True:
      rs = data.stdout.readline().strip()
      if rs:
        path = dir
        path += "/"
        path += rs
        rs_fd = open(path, "r")
        for line in rs_fd:
          line = line.strip()
          org_list = re.findall('(\w+)=\[(.*?)\]', line)
          map = dict(org_list)
          check_whether_exist_sql = "select * from RS_PROFILE where trace_id= %s and source_chid=%s" % (map["trace_id"], map["source_chid"])
          cursor.execute(check_whether_exist_sql)
          res = cursor.fetchone()
          if type(res) is types.NoneType:
            insert_sql = "insert into RS_PROFILE(trace_id, source_chid) values(%s, %s)" % (map["trace_id"], map["source_chid"])
            cursor.execute(insert_sql)
            connection.commit()
          update_sql = "update rs_profile set "
          flag = False
          for key in map:
            if key == "trace_id" or key == "source_chid" or key == "chid":
              continue
            else:
              if flag == False:
                if key == "self" or key == "peer":
                  update_sql += " rs_%s = '%s'" % (key, map[key])
                else:
                  update_sql += " rs_%s = %s" % (key, map[key])
                flag = True
              else:
                if key == "self" or key == "peer":
                  update_sql += ", rs_%s = '%s'" % (key, map[key])
                else:
                  update_sql += ",  rs_%s = %s" % (key, map[key])
          update_sql += " where trace_id = %s and source_chid = %s" % (map["trace_id"], map["source_chid"])
          cursor.execute(update_sql)
          connection.commit()
      else:
        break
  
  
if __name__ == "__main__":
  connect_db()
  download_files()
  aggregate_mergeserver()
  aggregate_chunkserver()
  aggregate_updateserver()
  aggregate_rootserver()
  #for k in sql_trace_map:
   # print sql_trace_map[k]
  #print sql_trace_map
  #generate_svg()
  #print "\n"
  #print inner_trace_map
