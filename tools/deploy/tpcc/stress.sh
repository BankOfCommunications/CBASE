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
    . $conf
    if [ "$type" == all ]; then
        pgrep -f ^$base_dir/tpcc_load >/dev/null && exit 1
        pgrep -f ^$base_dir/tpcc_start >/dev/null && exit 1

        arr=(${merge_server_str//,/ })  

        echo mergeserver list:
        for ms in ${arr[@]}  
        do  
            echo $ms 
        done  
        

        if [ $client_idx = "0" ];then
            echo "create tables ..."
            set -x
            mysql -h${arr[0]} -P${merge_server_mysql_port}  -u${username} -p${password} test < create_table.sql    
        
            echo "load data ..."
            sh load.sh test $warehouses ${arr[0]}:${merge_server_mysql_port}
            set +x
        fi
        
        ms=${arr[$client_idx]}
        set -x
            nohup $base_dir/tpcc_start -h ${ms} -P ${merge_server_mysql_port} -d test -u $username -p $password -w $warehouses -c $connections -r $warmup_time -l $running_time -i $report_interval -f $report_file -t $trx_file > $base_dir/tpcc_${client_idx}.log  2>&1 &

        set +x
        sleep 10
    else
        echo "operation[$type] not support!"
        return -1
    fi
}

stop()
{
    type=$1
    pkill -9 -f ^$base_dir/tpcc_start
}

check()
{
    #return 0
    type=$1
    pgrep -f ^$base_dir/tpcc_start >/dev/null || (echo 'no tpcc running!' && return 1; )
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
