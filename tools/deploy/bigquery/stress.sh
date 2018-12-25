#!/bin/bash
help()
{
    echo "Usages:"
    echo " ./stress.sh start type conf"
    echo " ./stress.sh clear"
    echo " ./stress.sh stop"
    echo " ./stress.sh check"
}

start()
{
    type=$1
    conf=$2
    if [ "$type" == all ]; then
      if ! pgrep -f ^$base_dir/bigquery >/dev/null; then
        nohup $base_dir/bigquery -f $conf >/dev/null 2>&1 &
      fi
      else
        echo "operation[$type] not support!"
        return -1
    fi
}

stop()
{
    type=$1
    pkill -f ^$base_dir/bigquery
}

check()
{
    type=$1
    pgrep -f ^$base_dir/bigquery >/dev/null || (echo 'no bigquery running!' && return 1; )
}

check_correct()
{
    if grep ERROR *.log>/dev/null; then
        echo "ERROR on $(hostname)"
        grep ERROR *.log|tail -3
        return 0
    else
        return 0
    fi
}

clear()
{
    rm -rf log core.*
}

real_file=`readlink -f $0`
cd `dirname $real_file`
base_dir="`pwd`"
export LD_LIBRARY_PATH=$base_dir/../lib/
ulimit -c unlimited
method=${1:-help}
shift
$method $*
