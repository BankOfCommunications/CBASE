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
    pgrep -f ^$base_dir/sysbench_mt >/dev/null && exit 1
    simon_args="--oltp-simon-host=$simon_host --oltp-simon-port=$simon_port --oltp-simon-cluster=$simon_cluster"
    privilege_args="--mysql-user=$ob_user --mysql-password=$ob_password --oltp-schema-path=$base_dir/fin.sql $simon_args"

    let table_size=$cs_rows
    prepare_cs_arg="--test=oltp  --oltp-table-size=$table_size --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on --oltp-insert-row=1000" 
    let table_size=$ups_rows+$cs_rows
    prepare_ups_arg="--test=oltp  --oltp-table-size=$table_size --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on --oltp-insert-row=1000" 

    args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-skip-trx=on --oltp-test-mode=complex --oltp-range-size=$range_size --oltp-schema-path=$base_dir/fin.sql"
    let 'rowoffset=client_idx*500000000'
    if [ "$type" == all ]; then
        if [ $client_idx = "0" ];then
            set -x
            #echo "[Prepare CS ROWS]"
            let 'tt=table_size+rowoffset'
            $base_dir/sysbench_mt  $privilege_args $prepare_cs_arg --oltp-row-offset=$rowoffset --oltp-table-size=$tt prepare
            #echo "[Start Merge]"
            #$base_dir/ups_admin  -a $ups_ip -p $ups_port -t major_freeze
            #echo "[Prepare UPS ROWS]"
            #$base_dir/sysbench_mt  $privilege_args $prepare_ups_arg --oltp-row-offset=$cs_rows prepare
            set +x
        fi
        echo "Run test backend: $base_dir/sysbench_mt  $privilege_args $prepare_ups_arg $args --oltp-row-offset=$rowoffset run > $base_dir/histest_${client_idx}.log"
        set -x
        nohup $base_dir/sysbench_mt $privilege_args $prepare_ups_arg $args $simon_args --oltp-row-offset=$rowoffset run >$base_dir/histest_${client_idx}.log 2>&1 &
        set +x
        sleep 10
    fi
    if [ "$type" == prepare ]; then
        if [ $client_idx = "0" ];then
            set -x
            #echo "[Prepare CS ROWS]"
            let 'tt=table_size+rowoffset'
            $base_dir/sysbench_mt  $privilege_args $prepare_cs_arg --oltp-row-offset=$rowoffset --oltp-table-size=$tt prepare
            #echo "[Start Merge]"
            #$base_dir/ups_admin  -a $ups_ip -p $ups_port -t major_freeze
            #echo "[Prepare UPS ROWS]"
            #$base_dir/sysbench_mt  $privilege_args $prepare_ups_arg --oltp-row-offset=$cs_rows prepare
            set +x
        fi
    fi
    if [ "$type" == run ]; then
        echo "Run test backend: $base_dir/sysbench_mt  $privilege_args $prepare_ups_arg $args --oltp-row-offset=$rowoffset run > $base_dir/histest_${client_idx}.log"
        set -x
        nohup $base_dir/sysbench_mt $privilege_args $prepare_ups_arg $args $simon_args --oltp-row-offset=$rowoffset run >$base_dir/histest_${client_idx}.log 2>&1 &
        set +x
        sleep 10
	
    fi
}

stop()
{
    type=$1
    pkill -9 -f ^$base_dir/sysbench_mt
}

check()
{
    #return 0
    type=$1
    pgrep -f ^$base_dir/sysbench_mt >/dev/null || (echo 'no sysbench_mt running!' && return 1; )
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
