#!/bin/bash 
help()
{
    echo "Usages:"
    echo " ./stress.sh start type conf"
    echo " ./stress.sh clear"
    echo " ./stress.sh stop"
    echo " ./stress.sh check"
}


oprepare()
{
    type=$1
    conf=$2
    . $conf
    if [ "$type" == all ]; then
        pgrep -f ^$base_dir/sysbench >/dev/null && exit 1

        simon_args="--oltp-simon-host=$simon_host --oltp-simon-port=$simon_port --oltp-simon-cluster=$simon_cluster"
        privilege_args="--mysql-user=$ob_user --mysql-password=$ob_password --oltp-schema-path=$base_dir/${case}.sql"
        prepare_cs_arg="--test=oltp  --oltp-table-size=$cs_rows --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=$merge_server_str --db-ps-mode=$ps_mode --ob=on --oltp-insert-row=1000" 
        
        set -x
        #LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH $base_dir/sysbench  $privilege_args $prepare_cs_arg --oltp-row-offset=0 prepare
        set +x
        err=$?
        if [ $err -eq 0 ]
        then
          #$base_dir/ups_admin -a ${ups_ip} -p ${ups_port} -t major_freeze
          #$base_dir/rs_admin -r ${ups_ip} -p 3202 check_tablet_version -o tablet_version=2
          #sleep 10
          return 0
        else
          return $err
        fi
    else
        echo "operation[$type] not support!"
        return -1
    fi
}

prepare()
{
  type=$1
  conf=$2
  . $conf
  options="--host=${merge_server_str[`expr ${client_idx} % ${merge_server_count}`]} --port=$merge_server_mysql_port --user=$ob_user --passwd=$ob_password --db= --config=$base_dir/${case}.lua --client_id=${client_idx}"
  echo "Run $base_dir/mysql_stress $options prepare > mysql_stress.out 2>&1 < /dev/zero"
  env LD_LIBRARY_PATH=.:/home/yanran.hfs/code/tblib/lib:$LD_LIBRARY_PATH $base_dir/mysql_stress $options prepare > mysql_stress.out 2>&1 < /dev/zero
  err=$?
  if [ $err -eq 0 ]
  then
    $base_dir/ups_admin -a ${ups_ip} -p ${ups_port} -t major_freeze
    sleep 10
  else
    return $err
  fi
}

ostart()
{
    type=$1
    conf=$2
    . $conf
    if [ "$type" == all ]; then
        pgrep -f ^$base_dir/sysbench >/dev/null && exit 1

        simon_args="--oltp-simon-host=$simon_host --oltp-simon-port=$simon_port --oltp-simon-cluster=$simon_cluster"
        privilege_args="--mysql-user=$ob_user --mysql-password=$ob_password --oltp-schema-path=$base_dir/${case}.sql"
        args="--test=oltp --oltp-table-size=$cs_rows --oltp-auto-inc=off --mysql-port=$merge_server_mysql_port --mysql-host=${merge_server_str[`expr ${client_idx} % ${merge_server_count}`]} --db-ps-mode=$ps_mode --ob=on --max-time=$max_seconds --max-requests=$max_requests --num-threads=$test_threads  --oltp-dist-type=uniform --oltp-skip-trx=on --oltp-test-mode=complex --oltp-range-size=$range_size --oltp-schema-path=$base_dir/${case}.sql"

        echo "Run test backend: LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH $base_dir/sysbench $privilege_args $args --oltp-row-offset=0 run >$base_dir/perf_${client_idx}.log 2>&1 &"
        set -x
        env LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH nohup $base_dir/sysbench $privilege_args $args --oltp-row-offset=0 run >$base_dir/perf_${client_idx}.log 2>&1 &
        set +x
        sleep 3
    else
        echo "operation[$type] not support!"
        return -1
    fi
}

start()
{
  type=$1
  conf=$2
  . $conf
  options="--running_time=$max_seconds --num_threads=$test_threads --host=${merge_server_str[`expr ${client_idx} % ${merge_server_count}`]} --port=$merge_server_mysql_port --user=$ob_user --passwd=$ob_password --db= --config=$base_dir/${case}.lua --client_id=${client_idx}"
  echo "Run $base_dir/mysql_stress $options stress"
  env LD_LIBRARY_PATH=.:/home/yanran.hfs/code/tblib/lib:$LD_LIBRARY_PATH $base_dir/mysql_stress $options stress > mysql_stress.out 2>&1 < /dev/zero &
}

stop()
{
    type=$1
    pkill -9 -f ^$base_dir/sysbench
    pkill -f ^$base_dir/mysql_stress
}

check()
{
    #return 0
    type=$1
    pgrep -f ^$base_dir/sysbench >/dev/null || pgrep -f ^$base_dir/mysql_stress >/dev/null || (echo 'no sysbench running!' && return 1; )
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
