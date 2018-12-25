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
        pgrep -f ^$base_dir/trxtest >/dev/null && exit 1

        privilege_args="--mysql-user=$ob_user --mysql-password=$ob_password"
        simon_args="--oltp-simon-host=$simon_host --oltp-simon-port=$simon_port --oltp-simon-cluster=$simon_cluster"

        args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads --oltp-range-size=$range_size --oltp-point-selects=$point_query --oltp-simple-ranges=$range_query --oltp-sum-ranges=$range_sum_query --oltp-order-ranges=$range_order_query --oltp-distinct-ranges=$range_distinct_query --oltp-join-point-selects=$point_join_query --oltp-join-simple-ranges=$range_join_query --oltp-join-sum-ranges=$range_sum_join_query --oltp-join-order-ranges=$range_order_join_query --oltp-join-distinct-ranges=$range_distinct_join_query --oltp-index-updates=$int_update --oltp-non-index-updates=$str_update"

        let table_size=$cs_rows
        prepare_cs_arg="--test=trx  --oltp-table-size=$table_size --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on" 
        let table_size=$ups_rows+$cs_rows
        prepare_ups_arg="--test=trx  --oltp-table-size=$table_size --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on" 
        
        if [ $client_idx = "0" ];then
            set -x
            echo "Clean up ..."
            $base_dir/trxtest  $privilege_args $prepare_cs_arg cleanup 
            echo "Prepare cs rows ..."
            $base_dir/trxtest  $privilege_args $prepare_cs_arg prepare
            echo "Start Merge ... "
            $base_dir/ups_admin  -a $ups_ip -p $ups_port -t major_freeze
            echo "Prepare ups rows ..."
            $base_dir/trxtest  $privilege_args $prepare_ups_arg --oltp-row-offset=$cs_rows prepare
            set +x
        fi
        
        for i in {1..5}
        do
        $base_dir/trxtest $privilege_args $prepare_ups_arg $args $simon_args --max-time=1800 --oltp-dist-type=uniform run | tee $base_dir/trxtest_warmup_${i}_${client_idx}.log 
#        $base_dir/ups_admin  -a $ups_ip -p $ups_port -t major_freeze
        done

        echo "Run test backend: $base_dir/trxtest  $privilege_args $prepare_ups_arg $args --oltp-dist-type=uniform  run > $base_dir/trxtest_${client_idx}.log"

        set -x
        nohup $base_dir/trxtest $privilege_args $prepare_ups_arg $args $simon_args --oltp-dist-type=uniform run >$base_dir/trxtest_${client_idx}.log 2>&1 &
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
    pkill -9 -f ^$base_dir/trxtest
}

check()
{
    #return 0
    type=$1
    pgrep -f ^$base_dir/trxtest >/dev/null || (echo 'no trxtest running!' && return 1; )
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
