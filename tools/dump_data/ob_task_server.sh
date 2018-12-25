#!/bin/sh
#BINDIR=/home/yushun.swh/ob_export_2
BINDIR=$(cd `dirname $0`;pwd)
#BINDIR=/home/yushun.swh/dev/oceanbase/tools/dump_data/

if [ $# -le 2 ]
then
  echo "ob_task_server.sh task_id outputpath task_list args"
  exit 1
fi

SERVER_CMD=${BINDIR}/task_server
CLEANUP_CMD=${BINDIR}/obcleanup.py

echo "ARGS:"$@

skynet_id=$1
shift 1

outpath=$1/_data_tmp
shift 1

task_list=$1
shift 1

#start task server
${SERVER_CMD} $@
${CLEANUP_CMD} hadoop ${task_list} ${outpath} decreped

