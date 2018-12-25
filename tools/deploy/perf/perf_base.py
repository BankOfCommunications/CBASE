# -*- coding: utf-8 -*-

import time
import datetime
try:
  import matplotlib.pyplot as plt
except ImportError:
  plt = None

def perf_client_attrs():
    prepare_data = '''sh: ssh $usr@$ip "${client_env_vars} client_idx=$idx $dir/$client/stress.sh prepare ${type} ${client_start_args}"'''
    return locals()

def perf_ct_attrs():
    prepare_data = 'all: client prepare_data type=all'
    reboot_to_prepare_data = 'seq: stop conf rsync clear prepare_data'
    return locals()

def perf_role_attrs():
    save_profile = '''sh: scp $usr@$ip:$dir/log/$role.profile $_rest_'''
    save_gprofile = '''sh: scp $usr@$ip:$gprofiler_output $_rest_'''
    start_periodic_pstack = '''sh: ssh $usr@$ip "sh $dir/tools/periodic_pstack.sh $__self__ \`pgrep -f ^$exe\` $dir/log/pstacks/ $ptime > /dev/null 2>&1 < /dev/zero &"'''
    save_periodic_pstack = '''sh: scp -r $usr@$ip:$dir/log/pstacks/$__self__ $_rest_'''
    return locals()

def perf_obi_attrs():
    perf_load_params = r'''call: obmysql extra="-e \"alter system set merge_delay_interval='1s' server_type=chunkserver;\""'''
    perf_create_tables = r'''call: obmysql extra="< perf/${case}.create"'''
    perf_init = 'seq: perf_load_params perf_create_tables sleep[sleep_time=2]'
    perf = 'seq: reboot sleep[sleep_time=10] perf_init perf_prepare.reboot_to_prepare_data turn_on_perf perf_run.reboot sleep[sleep_time=$ptime] perf_run.stop turn_off_perf collect_perf'
    perf_ups = 'seq: reboot sleep[sleep_time=10] perf_init turn_on_perf perf_run.reboot sleep[sleep_time=$ptime] perf_run.stop turn_off_perf collect_perf'
    running_ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    local_tmp_dir = "tmp.%s" % (running_ts)
    def get_cluster_ips(*args, **ob):
      def get_server_ips(server_role):
        server_list = get_match_child(ob, server_role)
        return [find_attr(find_attr(ob, k), "ip") for k in server_list.keys()]
      cluster_ips = get_server_ips("mergeserver") + get_server_ips("chunkserver") + get_server_ips("updateserver") + get_server_ips("rootserver")
      seen = set()
      seen_add = seen.add
      return [ x for x in cluster_ips if x not in seen and not seen_add(x)]

    def turn_on_perf(*args, **ob):
      # turn on profile log and gprofiler
      all_do(ob, 'mergeserver', 'kill', '-50')
      all_do(ob, 'chunkserver', 'kill', '-50')
      all_do(ob, 'mergeserver', 'kill', '-60')
      all_do(ob, 'chunkserver', 'kill', '-60')
      all_do(ob, 'updateserver', 'kill', '-60')
      #call_(ob, 'ms0.kill', '-50')
      #call_(ob, 'ms0.kill', '-60')
      #call_(ob, 'cs0.kill', '-60')
      #call_(ob, 'ups0.kill', '-60')
      for ip in get_cluster_ips(*args, **ob):
        ssh(ip, sub2("$dir/tools/linuxmon_x64.bin time=${ptime}s wait=1 back=yes > $dir/server_stats 2>&1 < /dev/zero &", ob))
      #call_(ob, 'ms0.start_periodic_pstack')
      #call_(ob, 'cs0.start_periodic_pstack')
      #call_(ob, 'ups0.start_periodic_pstack')

    def turn_off_perf(*args, **ob):
      # turn off profile log and gprofiler
      all_do(ob, 'mergeserver', 'kill', '-51')
      all_do(ob, 'chunkserver', 'kill', '-51')
      all_do(ob, 'mergeserver', 'kill', '-61')
      all_do(ob, 'chunkserver', 'kill', '-61')
      all_do(ob, 'updateserver', 'kill', '-61')
      #call_(ob, 'ms0.kill', '-51')
      #call_(ob, 'ms0.kill', '-61')
      #call_(ob, 'cs0.kill', '-61')
      #call_(ob, 'ups0.kill', '-61')

    def pprof_gprofile_output(server_bin, profile_output, perf_result_dir, *args, **ob):
      st = time.time()
      cost_graph_name = "${role}_${ip}_cost_graph.pdf"
      cost_graph_path = "%s/%s" % (perf_result_dir, cost_graph_name)
      top50_cmd = sub2("pprof --text $server_bin $profile_output", locals())
      graph_cmd = sub2("pprof --pdf $server_bin --edgefraction=0 $profile_output > $cost_graph_path", locals())
      pro_res = popen(sub2(top50_cmd, ob)).splitlines()
      i = 0
      while i < len(pro_res):
        if pro_res[i].startswith('Total: '):
          i += 1
          break
        i += 1
      func_top50 = '\n'.join(pro_res[i:i + 50])
      sh(sub2(graph_cmd, ob))
      info('drawing %s profile graph costs %ss' % (server_bin, time.time() - st))
      output = """
<p>$role $ip 函数消耗排名前50：
<pre>%s</pre></p>
<p><a href="%s">函数消耗线框图</a></p>
""" % (func_top50, cost_graph_name)
      return sub2(output, ob)

    def parse_profile_log(profile_log, perf_result_dir, *args, **ob):
      time_format = "%Y-%m-%d %H:%M:%S"
      query_ratio = int(sub2("$ptime", ob), 10)
      d = dict()
      start_time = None
      end_time = None
      sql_count = 0
      real_sql_count = 0
      sql_time = 0
      sql_time_dist = dict()
      wait_time = 0
      ud = dict(sql_count = 0, real_sql_count = 0, sql_time = 0, wait_time = 0)
      qps2time = dict()
      rpcs = dict()
      rpcs_html = ""
      wait_times = []
      parse_log_st = time.time()
      def get_packet_name(pcode):
        if pcode == 4002: return "OB_PHY_PLAN_EXECUTE"
        elif pcode == 409: return "OB_SQL_GET_REQUEST"
        elif pcode == 405: return "OB_SQL_SCAN_REQUEST"
        else: return "OB_UNKNOWN_PACKET"
      def add_sql(trace_id, ts, sqlt, waitt, rpc_list):
        if qps2time.has_key(ts):
          qps2time[ts] += query_ratio
        else:
          qps2time[ts] = 0
        ud['sql_count'] += query_ratio
        ud['real_sql_count'] += 1
        ud['sql_time'] += sqlt
        if sql_time_dist.has_key(sqlt):
          sql_time_dist[sqlt] += query_ratio
        else:
          sql_time_dist[sqlt] = 0
        ud['wait_time'] += waitt
        wait_times.append(waitt)
        for rpc in rpc_list:
          if rpcs.has_key(rpc['pcode']):
            rpcs[rpc['pcode']]['rpc_time'] += rpc['latency']
            rpcs[rpc['pcode']]['rpc_times'].append(rpc['latency'])
          else:
            rpcs[rpc['pcode']] = dict(rpc_time = rpc['latency'], rpc_times = [rpc['latency']])

      with open(sub2(profile_log, ob)) as f:
        for l in f:
          m = re.search(r'trace_id=\[(\d+)\] sql=\[.*\] sql_to_logicalplan=\[\d+\] logicalplan_to_physicalplan=\[\d+\] handle_sql_time=\[(\d+)\] wait_sql_queue_time=\[(\d+)\] rpc:channel_id=\[\d+\] rpc_start=\[\d+\] latency=\[(\d+)\] pcode=\[(\d+)\] sql_queue_size=\[\d+\] print_time=\[(\d+)\]', l)
          if m is not None:
            end_time = int(m.group(6))
            trace_id = m.group(1)
            ts = m.group(6)[:-6]
            sql_time  = int(m.group(2))
            wait_time = int(m.group(3))
            rpc_list = [dict(pcode = int(m.group(5)), latency = int(m.group(4)))]
            add_sql(trace_id, ts, sql_time, wait_time, rpc_list)
          else:
            m = re.search(r'trace_id=\[(\d+)\] sql=\[.*\] sql_to_logicalplan=\[\d+\] logicalplan_to_physicalplan=\[\d+\] handle_sql_time=\[(\d+)\] wait_sql_queue_time=\[(\d+)\] rpc:channel_id=\[\d+\] rpc_start=\[\d+\] latency=\[(\d+)\] pcode=\[(\d+)\] rpc:channel_id=\[\d+\] rpc_start=\[\d+\] latency=\[(\d+)\] pcode=\[(\d+)\] sql_queue_size=\[\d+\] print_time=\[(\d+)\]', l)
            if m is not None:
              end_time = int(m.group(8))
              trace_id = m.group(1)
              ts = m.group(8)[:-6]
              sql_time  = int(m.group(2))
              wait_time = int(m.group(3))
              rpc_list = [dict(pcode = int(m.group(5)), latency = int(m.group(4))),
                          dict(pcode = int(m.group(7)), latency = int(m.group(6))),]
              add_sql(trace_id, ts, sql_time, wait_time, rpc_list)
            else:
              m = re.search(r'trace_id=\[(\d+)\] sql=\[.*\] sql_to_logicalplan=\[\d+\] logicalplan_to_physicalplan=\[\d+\] handle_sql_time=\[(\d+)\] wait_sql_queue_time=\[(\d+)\] rpc:channel_id=\[\d+\] rpc_start=\[\d+\] latency=\[(\d+)\] pcode=\[(\d+)\] rpc:channel_id=\[\d+\] rpc_start=\[\d+\] latency=\[(\d+)\] pcode=\[(\d+)\] rpc:channel_id=\[\d+\] rpc_start=\[\d+\] latency=\[(\d+)\] pcode=\[(\d+)\] sql_queue_size=\[\d+\] print_time=\[(\d+)\]', l)
              if m is not None:
                end_time = int(m.group(10))
                trace_id = m.group(1)
                ts = m.group(10)[:-6]
                sql_time  = int(m.group(2))
                wait_time = int(m.group(3))
                rpc_list = [dict(pcode = int(m.group(5)), latency = int(m.group(4))),
                            dict(pcode = int(m.group(7)), latency = int(m.group(6))),
                            dict(pcode = int(m.group(9)), latency = int(m.group(8))),]
                add_sql(trace_id, ts, sql_time, wait_time, rpc_list)
          if start_time is None and end_time is not None:
            start_time = end_time
      info("parsing log costs %ss" % (time.time() - parse_log_st))
      sql_time = ud['sql_time']
      sql_count = ud['sql_count']
      real_sql_count = ud['real_sql_count']
      wait_time = ud['wait_time']
      drawing_st = time.time()
      if end_time is None:
        elapsed_seconds = 0
        qps = 0
        avg_sql_time = 0
        avg_wait_time = 0
        for pcode, rpc in rpcs.items():
          rpc['avg_rpc_time'] = 0
      else:
        elapsed_seconds = (end_time - start_time) / 10**6
        if elapsed_seconds > 0:
          qps = sql_count / elapsed_seconds
          avg_sql_time = float(sql_time) / real_sql_count
          avg_wait_time = float(wait_time) / real_sql_count
        else:
          qps = 0
          avg_sql_time = 0
          avg_wait_time = 0
        for pcode, rpc in rpcs.items():
          rpc['avg_rpc_time'] = float(rpc['rpc_time']) / len(rpc['rpc_times'])
        if plt is not None:
          plt.plot([x for k,x in sorted(qps2time.items())], '-')
          plt.xlabel('Timeline')
          plt.ylabel('QPS')
          plt.savefig(sub2("%s/${role}_${ip}_qps.png" % (perf_result_dir), ob))
          plt.clf()
          plt.bar(sql_time_dist.keys(), sql_time_dist.values())
          plt.xlabel('Response Time (us)')
          plt.ylabel('Number of Requests')
          plt.savefig(sub2("%s/${role}_${ip}_total_time_dist.png" % (perf_result_dir), ob))
          plt.clf()
          plt.plot(wait_times, ',')
          plt.xlabel('Timeline')
          plt.ylabel('Wait Time in Mergeserver Queue (us)')
          plt.savefig(sub2("%s/${role}_${ip}_queue_time.png" % (perf_result_dir), ob))
          plt.clf()
          for pcode, rpc in rpcs.items():
            plt.plot(rpc['rpc_times'], ',')
            plt.xlabel('Timeline')
            plt.ylabel('Response Time (us)')
            plt.savefig(sub2("%s/${role}_${ip}_%s.png" % (perf_result_dir, pcode), ob))
            plt.clf()
            rpcs_html += sub2("""<p>$role $ip %s(%s)请求次数：%s  平均响应延时：%sus<br><img src="${role}_${ip}_%s.png" /></p>"""
                          % (pcode, get_packet_name(pcode), len(rpc['rpc_times']), rpc['avg_rpc_time'], pcode), ob)
      info("drawing performance graph costs %ss" % (time.time() - drawing_st))
      parse_perf = sub2(sub2("""
<p> ${role} ${ip} 测试运行时间：${elapsed_seconds}s<br>
${role} ${ip} SQL请求次数：$sql_count<br>
${role} ${ip} MS的QPS：$qps</p>
<p>${role} ${ip} QPS：<br>
<img src="${role}_${ip}_qps.png" /></p>
<p>${role} ${ip} 平均响应延时：${avg_sql_time}us<br>
<img src="${role}_${ip}_total_time_dist.png" /></p>
<p>${role} ${ip} MS队列中平均花费的时间：${avg_wait_time}us<br>
<img src="${role}_${ip}_queue_time.png" /></p>
$rpcs_html
""", locals()), ob)
      return dict(
                  parse_res = parse_perf,
                  stat = dict(
                              sql_count = sql_count,
                              real_sql_count = real_sql_count,
                              elapsed_seconds = elapsed_seconds,
                              sql_time = sql_time,
                              wait_time = wait_time,
                              qps2time = qps2time,
                              sql_time_dist = sql_time_dist,
                              wait_times = wait_times,
                              rpcs = rpcs,
                             )
                 )

    def collect_perf(*args, **ob):
      def get_server_list(server_role):
        server_list = get_match_child(ob, server_role)
        server_list_str = ' '.join('${%s.ip}:${%s.port}'%(k, k) for k in server_list.keys())
        return server_list_str

      perf_result_dir = "/home/yanran.hfs/public_html/ob_perf/not_published/%s" % (running_ts)
      os.mkdir(perf_result_dir)
      os.mkdir(local_tmp_dir)

      ms_profile = '%s/$role.$ip.profile' % (local_tmp_dir)

      all_do(ob, 'mergeserver', 'save_gprofile', local_tmp_dir)
      all_do(ob, 'chunkserver', 'save_gprofile', local_tmp_dir)
      all_do(ob, 'updateserver', 'save_gprofile', local_tmp_dir)
      all_do(ob, 'mergeserver', 'save_profile', ms_profile)
      #call_(ob, 'ms0.save_periodic_pstack', perf_result_dir)
      #call_(ob, 'cs0.save_periodic_pstack', perf_result_dir)
      #call_(ob, 'ups0.save_periodic_pstack', perf_result_dir)

      ms_gprofile_output = "%s/%s" % (local_tmp_dir, str.split(find_attr(ob, "ms0.gprofiler_output"), '/')[-1])
      cs_gprofile_output = "%s/%s" % (local_tmp_dir, str.split(find_attr(ob, "cs0.gprofiler_output"), '/')[-1])
      ups_gprofile_output = "%s/%s" % (local_tmp_dir, str.split(find_attr(ob, "ups0.gprofiler_output"), '/')[-1])
      ms_gprof = all_do(ob, 'mergeserver', 'pprof_gprofile_output', "bin/mergeserver", ms_gprofile_output, perf_result_dir)
      cs_gprof = all_do(ob, 'chunkserver', 'pprof_gprofile_output', "bin/chunkserver", cs_gprofile_output, perf_result_dir)
      ups_gprof = all_do(ob, 'updateserver', 'pprof_gprofile_output', "bin/updateserver", ups_gprofile_output, perf_result_dir)
      ms_gprof = ''.join([x[1] for x in ms_gprof])
      cs_gprof = ''.join([x[1] for x in cs_gprof])
      ups_gprof = ''.join([x[1] for x in ups_gprof])

      ms_prof = all_do(ob, 'mergeserver', 'parse_profile_log', ms_profile, perf_result_dir)
      ms_prof_htmls = ''.join([x[1]['parse_res'] for x in ms_prof])
      sql_count = 0
      real_sql_count = 0
      elapsed_seconds = 0
      sql_time = 0
      wait_time = 0
      qps2time = None
      sql_time_dist = None
      for ms_tuple in ms_prof:
        ms = ms_tuple[1]['stat']
        if elapsed_seconds < ms['elapsed_seconds']:
          elapsed_seconds = ms['elapsed_seconds']
        sql_count += ms['sql_count']
        real_sql_count += ms['real_sql_count']
        sql_time += ms['sql_time']
        wait_time += ms['wait_time']
        qps2time = dict_add(qps2time, ms['qps2time'])
        sql_time_dist = dict_add(sql_time_dist, ms['sql_time_dist'])
      if elapsed_seconds == 0:
        qps = 0
        avg_sql_time = 0
      else:
        qps = sql_count / elapsed_seconds
        avg_sql_time = float(sql_time) / real_sql_count
        if plt is not None:
          plt.plot([x for k,x in sorted(qps2time.items())], '-')
          plt.xlabel('Timeline')
          plt.ylabel('QPS')
          plt.savefig("%s/cluster_qps.png" % (perf_result_dir))
          plt.clf()
          plt.bar(sql_time_dist.keys(), sql_time_dist.values())
          plt.xlabel('Response Time (us)')
          plt.ylabel('Number of Requests')
          plt.savefig("%s/cluster_total_time_dist.png" % (perf_result_dir))
          plt.clf()

      user_name = find_attr(ob, "usr")
      case_name = find_attr(ob, "case")
      rs_list = get_server_list("rootserver")
      ups_list = get_server_list("updateserver")
      ms_list = get_server_list("mergeserver")
      cs_list = get_server_list("chunkserver")
      server_stats = ""
      for ip in get_cluster_ips(*args, **ob):
        sh(sub2('scp $usr@%s:$dir/server_stats %s/' % (ip, local_tmp_dir), ob))
        server_stats += "<p>%s监控信息：<pre>%s</pre></p>" % (ip, read("%s/server_stats" % (local_tmp_dir)))
      result_html_template = ("""
<p>测试人员：${user_name}<br>
测试Case：${case_name}<br>
运行环境：
<ul>
  <li>RootServer: ${rs_list}</li>
  <li>UpdateServer: ${ups_list}</li>
  <li>MergeServer: ${ms_list}</li>
  <li>ChunkServer: ${cs_list}</li>
</ul>
测试运行时间：${elapsed_seconds}s<br>
SQL请求次数：$sql_count<br>
集群QPS：$qps</p>
<p>QPS：<br>
<img src="cluster_qps.png" /></p>
<p>平均响应延时：${avg_sql_time}us<br>
<img src="cluster_total_time_dist.png" /></p>
${ms_prof_htmls}
${ms_gprof}
${cs_gprof}
${ups_gprof}
${server_stats}
<p>Server栈信息：
<ul>
<li><a href="ms0">ms0</a></li>
<li><a href="cs0">cs0</a></li>
<li><a href="ups0">ups0</a></li>
</ul></p>
""")
      all_vars = copy.copy(ob)
      all_vars.update(locals())
      with open("%s/index.html" % (perf_result_dir), "w") as f:
        f.write(sub2(result_html_template, all_vars))
      with open("/home/yanran.hfs/public_html/ob_perf/not_published/index.html", "a") as f:
        f.write("""<li><a href="%s/">%s %s %s</a></li>\n""" % (running_ts, running_ts, user_name, case_name))
      sh("rm -r %s" % (local_tmp_dir))

    return locals()

def perf_environ_setup():
  #ob['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
  #ob['gprofiler_output']  = "$dir/$ip.gprofiler.output"
  #ob['environ_extras']    = "PROFILE_SAMPLE_INTERVAL=$ptime"
  #ob['ms0']['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
  #ob['ms0']['gprofiler_output'] = "$dir/ms0.gprofiler.output"
  #ob['ms0']['environ_extras'] = sub2("PROFILEOUTPUT=$gprofiler_output PROFILE_SAMPLE_INTERVAL=$ptime", ob)
  #ob['cs0']['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
  #ob['cs0']['gprofiler_output'] = "$dir/cs0.gprofiler.output"
  #ob['cs0']['environ_extras'] = "PROFILEOUTPUT=$gprofiler_output"
  #ob['ups0']['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
  #ob['ups0']['gprofiler_output'] = "$dir/ups0.gprofiler.output"
  #ob['ups0']['environ_extras'] = "PROFILEOUTPUT=$gprofiler_output"
  #obi_vars.update(dict(environ_extras = "PROFILE_SAMPLE_INTERVAL=$ptime"))
  obi_vars['gprofiler_output']  = "$dir/$ip.$role.gprofiler.output"
  obi_vars['environ_extras']    = "PROFILEOUTPUT=$gprofiler_output PROFILE_SAMPLE_INTERVAL=$ptime"
  obi_vars['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
  #call_(ob, 'ms0.kill', '-50')

def perf_install():
  client_vars.update(dict_filter_out_special_attrs(perf_client_attrs()))
  ct_vars.update(dict_filter_out_special_attrs(perf_ct_attrs()))
  role_vars.update(dict_filter_out_special_attrs(perf_role_attrs()))
  obi_vars.update(dict_filter_out_special_attrs(perf_obi_attrs()))

perf_environ_setup()
perf_install()

