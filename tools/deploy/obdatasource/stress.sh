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
    if [ "$type" == all ]; then
        echo "start"
        conf=`echo $@ | cut -d" " -f2`
        . $conf
        c=`echo $@ | cut -d" " -f5`
        s2=`echo $@ | cut -d" " -f3` # post
        s1=`echo $@ | cut -d" " -f4` # prev
        if [ $client_idx = "0" ];then
            set -x
            echo "Prepare cs rows ..."
            $base_dir/obmonster -c $c -s $s1 -t $case
            $base_dir/ups_admin  -a $ups_ip -p $ups_port -t major_freeze
            echo "Prepare ups rows ..."
            $base_dir/obmonster -c $c -s $s2 -t $case
            set +x
        fi
        
        nohup $base_dir/obmonster -c $c -s $s2 -t $case -a run  > $base_dir/client_${client_idx}.log 2>&1 &
        sleep 10
    else
        echo "operation[$type] not support!"
        return -1
    fi
}

stop()
{
    type=$1
    pkill -9 -f "java -cp ./obmonster.jar"
}

check()
{
    #return 0
    type=$1
    pgrep -f ^$base_dir/obmonster >/dev/null || (echo 'no sysbench running!' && return 1; )
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
    rm -rf *.log core.*
}

real_file=`readlink -f $0`
cd `dirname $real_file`
base_dir="`pwd`"
ulimit -c unlimited
method=${1:-help}
shift
$method $*
