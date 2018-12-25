#!/bin/bash
help()
{
    echo "Usages:"
    echo " ./stress.sh start rs-ip rs-port"
    echo " ./stress.sh clear"
    echo " ./stress.sh stop"
    echo " ./stress.sh check"
}

start()
{
    type=$1
    nthreads=$2
    ip=$3
    port=$4
    if [ "$type" == write ]; then
        (pgrep -f ^$base_dir/multi_write >/dev/null || $base_dir/launcher -a $ip -p $port -t write -n $nthreads) >/dev/null 2>&1 &
    elif [ "$type" == mget ]; then
        (pgrep -f ^$base_dir/random_read >/dev/null || $base_dir/launcher -a $ip -p $port -t random_mget -n $nthreads -k) >/dev/null 2>&1 &
    elif [ "$type" == all ]; then
        (pgrep -f ^$base_dir/multi_write || pgrep -f ^$base_dir/random_read  || is_read_consistency=$is_read_consistency nohup $base_dir/launcher -a $ip -p $port -t mixed -n $nthreads -k) >/dev/null 2>&1 &
    else
        echo "operation[$type] not support!"
        return 1
    fi
}

stop()
{
    type=$1
    if [ "$type" == write ]; then
        pkill -9 -f ^$base_dir/multi_write
    elif [ "$type" == mget ]; then
        pkill -9 -f ^$base_dir/random_read
    elif [ "$type" == all ]; then
        stop write; stop mget
    else
        echo "operation[$type] not support!"
        return 1
    fi
}

check()
{
    type=$1
    if [ "$type" == write ]; then
        pgrep -f ^$base_dir/multi_write >/dev/null || (echo 'no writer running!' && return 1; )
    elif [ "$type" == mget ]; then
        pgrep -f ^$base_dir/random_read >/dev/null || (echo 'no reader running!' && return 1; )
    elif [ "$type" == all ]; then
        check write && check mget
    else
        echo "operation[$type] not support!"
        return 1
    fi
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
    rm -f *.log* core.*
}

real_file=`readlink -f $0`
cd `dirname $real_file`
base_dir="`pwd`"
ulimit -c unlimited
ulimit -s unlimited
method=${1:-help}
shift
$method $*
