#!/bin/bash
help()
{
    echo "Usages:"
    echo " ./stress.sh start rs-ip:rs-port"
    echo " ./stress.sh clear"
    echo " ./stress.sh stop"
    echo " ./stress.sh check"
}
tail_()
{
    tail -f client.log
}

less_()
{
    less client.log
}

start()
{
    type=$1
    shift
    if [ "$type" == all ]; then
        echo "nohup $base_dir/client stress $rs $* >client.log 2>&1 &"
        nohup $base_dir/client stress $rs $* >client.log 2>&1 &
    else
        echo "operation[$type] not support!"
        return 1
    fi
}

stop()
{
    type=$1
    if [ "$type" == all ]; then
        pkill -9 -f ^$base_dir/client
    else
        echo "operation[$type] not support!"
        return 1
    fi
}

check()
{
    type=$1
    if [ "$type" == all ]; then
        pgrep -f ^$base_dir/client >/dev/null
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
method=${1:-help}
shift
$method $*
