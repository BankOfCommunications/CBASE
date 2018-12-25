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
        pgrep -f ^$base_dir/syschecker >/dev/null || $base_dir/syschecker -f $conf
    else
        echo "operation[$type] not support!"
        return -1
    fi
}

stop()
{
    type=$1
    pkill -9 -f ^$base_dir/syschecker
}

check()
{
    type=$1
    pgrep -f ^$base_dir/syschecker >/dev/null || (echo 'no syschecker running!' && return 1; )
}

check_correct()
{
    if grep ERROR log/*.log>/dev/null; then
        echo "ERROR on $(hostname)"
        grep ERROR log/*.log|tail -3
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
ulimit -c unlimited
method=${1:-help}
shift
$method $*
