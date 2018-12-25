#!/bin/sh
BINDIR=/home/yushun.swh/ob_export_2

if [ $# -le 1 ]
then
  echo "ob_task_server.sh task_id args"
  exit 1
fi

WORKER_CMD=${BINDIR}/task_worker

echo "ARGS:"$@

skynet_id=$1
shift 1

outpath=$1/_data_tmp
shift 1

#start task server
${WORKER_CMD} $@ -f ${outpath}

