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
        pgrep -f ^$base_dir/sysbench >/dev/null && exit 1

        # to check if all options are set to zero
        let total=$point_query+$range_query+$range_sum_query+$range_order_query+$range_distinct_query+$point_join_query+$range_join_query+$range_sum_join_query+$range_order_join_query+$range_distinct_join_query        

        privilege_args="--mysql-user=$ob_user --mysql-password=$ob_password --oltp-schema-path=$base_dir/oltp.sql"
        simon_args="--oltp-simon-host=$simon_host --oltp-simon-port=$simon_port --oltp-simon-cluster=$simon_cluster"

        if [ $test_mode = "mix" ];then
                 args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-skip-trx=on --oltp-test-mode=complex --oltp-range-size=$range_size --oltp-point-selects=$point_query --oltp-simple-ranges=$range_query --oltp-sum-ranges=$range_sum_query --oltp-order-ranges=$range_order_query --oltp-distinct-ranges=$range_distinct_query --oltp-join-point-selects=$point_join_query --oltp-join-simple-ranges=$range_join_query --oltp-join-sum-ranges=$range_sum_join_query --oltp-join-order-ranges=$range_order_join_query --oltp-join-distinct-ranges=$range_distinct_join_query --oltp-replaces=$replace_query  --oltp-index-updates=$update_query --oltp-deletes=$delete_query"
        elif [ $test_mode = "read" ];then
             if [ $total -eq 0 ];then
                 # use default 
                 args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-skip-trx=on --oltp-read-only=on --oltp-test-mode=complex --oltp-range-size=$range_size"
             else
                 args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-skip-trx=on --oltp-read-only=on --oltp-test-mode=complex --oltp-range-size=$range_size --oltp-point-selects=$point_query --oltp-simple-ranges=$range_query --oltp-sum-ranges=$range_sum_query --oltp-order-ranges=$range_order_query --oltp-distinct-ranges=$range_distinct_query --oltp-join-point-selects=$point_join_query --oltp-join-simple-ranges=$range_join_query --oltp-join-sum-ranges=$range_sum_join_query --oltp-join-order-ranges=$range_order_join_query --oltp-join-distinct-ranges=$range_distinct_join_query"
             fi
        elif [ $test_mode = "update" ];then
             args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-test-mode=nontrx --oltp-nontrx-mode=update_key"
        elif [ $test_mode = "insert" ];then
             args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-test-mode=nontrx --oltp-nontrx-mode=insert --oltp-table-size=2147483648"

        elif [ $test_mode = "replace" ];then
             args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-test-mode=nontrx --oltp-nontrx-mode=replace"
        elif [ $test_mode = "delete" ];then
             args="--max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-test-mode=nontrx --oltp-nontrx-mode=delete"
        else
             echo "Unknown test mode: " $test_mode
             return -1
        fi

        let table_size=$cs_rows
        prepare_cs_arg="--test=oltp  --oltp-table-size=$table_size --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on" 
        let table_size=$ups_rows+$cs_rows
        prepare_ups_arg="--test=oltp  --oltp-table-size=$table_size --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on" 
        
        if [ $client_idx = "0" ];then
            set -x
            #echo "Clean up ..."
            #$base_dir/sysbench  $privilege_args $prepare_cs_arg cleanup 
            echo "Prepare cs rows ..."
            $base_dir/sysbench  $privilege_args $prepare_cs_arg prepare
            echo "Start Merge if test mode is not insert... "
            [ "z$test_mode" = "zinsert" ] ||  $base_dir/ups_admin  -a $ups_ip -p $ups_port -t major_freeze
            echo "Prepare ups rows ..."
            $base_dir/sysbench  $privilege_args $prepare_ups_arg --oltp-row-offset=$cs_rows prepare
            set +x
        fi
        echo "Run test backend: $base_dir/sysbench  $privilege_args $prepare_ups_arg $args run > $base_dir/benchmark_${client_idx}.log"
        set -x
        nohup $base_dir/sysbench $privilege_args $prepare_ups_arg $args $simon_args run >$base_dir/benchmark_${client_idx}.log 2>&1 &
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
    pkill -9 -f ^$base_dir/sysbench
}

check()
{
    #return 0
    type=$1
    pgrep -f ^$base_dir/sysbench >/dev/null || (echo 'no sysbench running!' && return 1; )
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
