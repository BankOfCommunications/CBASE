#!/bin/bash
#set -x

HDFS_BASEDIR="/group/tbpre/obdump/collect"
HADOOP_EXE="hadoop"
BASEDIR="/home/admin/obdump/data_dir/data"
TABLES="
$1
"
host=`hostname`

DONE="DONE"
DATE=`date -d "1 days ago" +"%Y-%m-%d"`

function checkTable()
{
	dfs_done_check="hadoop dfs -test -e $HDFS_BASEDIR/$table/${DATE}/${host}.DONE"
	`$dfs_done_check`; ret=$?
	if [ $ret -ne 0 ]; then 
		typeset local table=$1;  
		if [ -e $BASEDIR/$table/$DATE/$DONE ]; then
			ls $BASEDIR/$table/$DATE | grep '^[[:digit:]]\+$' >/dev/null
			if [ $? -eq 1 ];
			then
				echo "OK: data ready"
				return 0;
			else
				echo "DFSWriter still hasn't put all files."
				return 1;
			fi
		else
			echo "DATA NOT DONE"
			return 1;
		fi
	else
		echo "There are exist done already!"
		return 1;
	fi
		 
}

function clearTable()
{
        typeset local table=$1;
        typeset local hdfs_path=${HDFS_BASEDIR}/$table/${DATE}/$host
        script="hadoop dfs -rmr ${hdfs_path}/*.tmp";
        `$script`
	#echo $script
}

function markDoneTable()
{
        typeset local table=$1;
        typeset local hdfs_path=${HDFS_BASEDIR}/$table/${DATE}
        script="hadoop dfs -touchz ${hdfs_path}/${host}.DONE";
        `$script`
}

echo $BASEDIR
echo $HDFS_BASEDIR

for table in $TABLES
do
	checkTable $table;ret=$?
        if [ $ret -eq 0 ];
        then
          #clearTable $table;
          markDoneTable $table;
        fi
done
