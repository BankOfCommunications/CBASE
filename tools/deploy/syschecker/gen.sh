#!/bin/sh
config=$1
data_dir=$2
app_name=$3
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:../lib:/usr/local/lib64
if [ -z $config ] || [ -z $data_dir ] || [ -z $app_name ]; then
    echo "Missing Argument: $*"; exit 1;
fi

real_file=`readlink -f $0`
cd `dirname $real_file`
rows_count=100000;
sstable_cnt=1;
rows_cnt_per_sstable=$(($rows_count / $sstable_cnt));
mkdir -p log
for i in {1..9}; do
  seed=$((($i - 1) * $rows_count))
  if [ $i -eq 1 ];then
    extra="-a"
  elif [ $i -eq 9 ]; then
    extra="-z"
  else
    extra=""
  fi
  sstable_dir=$data_dir/$i/$app_name/sstable
(rm -f $sstable_dir/*; ./gen_sstable -s $config -t 1001,1002 -m $rows_cnt_per_sstable -N $sstable_cnt -i $i -l $seed -x 0 -j -c snappy_1.0 -d $sstable_dir $extra >log/gen.log 2>&1)&
done

FAIL=0
for job in `jobs -p`; do
    wait $job || let "FAIL+=1"
done
if [ "$FAIL" == "0" ];  then
    echo "$(hostname) end generate sstable file SUCCESS: $*"
    exit 0
else
    grep ERROR log/gen.log|tail -3
    echo "$(hostname) generate sstable file FAIL: $*"
    exit 1
fi
