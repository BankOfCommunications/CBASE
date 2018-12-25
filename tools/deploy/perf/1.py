import datetime
import re
import sys
try:
  import matplotlib.pyplot as plt
except ImportError:
  plt = None

time_format = "%Y-%m-%d %H:%M:%S"
d = dict()
start_time = None
start_time = None
sql_count = 0
sql_time = 0
sql_time_dist = dict()
rpc_time = 0
urpc_time = 0
wait_time = 0
qps2time = dict()
rpc_times = []
urpc_times = []
wait_times = []
for l in sys.stdin:
  m = re.search(r'trace_id=\[(\d+)\] sql=\[.*\] sql_to_logicalplan=\[\d+\] logicalplan_to_physicalplan=\[\d+\] handle_sql_time=\[(\d+)\] wait_sql_queue_time=\[(\d+)\] rpc:channel_id=\[\d+\] latency=\[(\d+)\] print_time=\[(\d+)\]', l)
  if m is not None:
    end_time = int(m.group(5))
    if start_time is None:
      start_time = end_time
    trace_id = m.group(1)
    ts = m.group(5)[:-6]
    d[trace_id] = dict(
                       sql_time  = int(m.group(2)),
                       wait_time = int(m.group(3)),
                       rpc_time  = int(m.group(4)),
                      )
    sql_count += 1
    sql_time += d[trace_id]['sql_time']
    if sql_time_dist.has_key(d[trace_id]['sql_time']):
      sql_time_dist[d[trace_id]['sql_time']] += 1
    else:
      sql_time_dist[d[trace_id]['sql_time']] = 0
    wait_time += d[trace_id]['wait_time']
    wait_times.append(d[trace_id]['wait_time'])
    rpc_time += d[trace_id]['rpc_time']
    rpc_times.append(d[trace_id]['rpc_time'])
    if qps2time.has_key(ts):
      qps2time[ts] += 1
    else:
      qps2time[ts] = 0
elapsed_seconds = (end_time - start_time) / 10**6
qps = sql_count / elapsed_seconds
avg_sql_time = float(sql_time) / sql_count
avg_rpc_time = float(rpc_time) / sql_count
avg_urpc_time = float(urpc_time) / sql_count
avg_wait_time = float(wait_time) / sql_count

print "QPS: %d" % (qps)
print "AVG TIME: %f" % (avg_sql_time)
print "AVG RPC TIME: %f" % (avg_rpc_time)
print "AVG WAIT TIME: %f" % (avg_wait_time)

