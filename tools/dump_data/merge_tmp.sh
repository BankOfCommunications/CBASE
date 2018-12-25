#!/bin/sh

MERGE_JAR=${BINDIR}/obdumpmerge-3.1.jar

if [ $# -ne 1 ]
then
  echo "merge_tmp.sh data_dir"
  exit 0
fi

data_dir=$1
for dir in `hadoop fs -ls ${data_dir}`
do
  if hadoop fs -test -d ${dir}
  then
    echo "dir:"$dir
  fi
done
