#!/bin/bash

#####################################################################
# (C) 2010-2011 Taobao Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# dispatch.sh is for dispatch sstable files to multiple chunk server
#
# Authors:
#   huating <huating.zmq@taobao.com>  
#
#####################################################################

export LANG=en_CN.UTF-8

usage()
{
	echo "Desc : dispatch sstable files to multiple chunk servers"
	echo "Usage: $(basename $0) [-b] [-f] -m -c 'config_files' -g gateway_host_file"
	echo "       $(basename $0) [-b] [-f] -m -c 'config_files' 'gateway_hosts'"
	echo "       $(basename $0) [-b] [-f] -m -l config_list_file -g gateway_host_file"
	echo "       $(basename $0) [-b] [-f] -m -l config_list_file 'gateway_hosts'"
	echo "       $(basename $0) -b -c 'config_files'"
	echo "       $(basename $0) -b -l config_list_file"
	echo "       $(basename $0) -f -c 'config_files'"
	echo "       $(basename $0) -f -l config_list_file"
	echo "       $(basename $0) -c 'config_files' -i gateway_index -n gateway_count [-o cs_ip] [-d dead_gateway_index -s dead_gateway_ip]"
	echo "       $(basename $0) -l config_list_file -i gateway_index -n gateway_count [-o cs_ip] [-d dead_gateway_index -s dead_gateway_ip]"
	echo "Options:"
	echo "  -h                    show this help message and exit"
	echo "  -l CONFIG_LIST_FILE   specify table config list file"
	echo "  -c TABLE_CONFIG_FILES specify config files of table to dispath"
	echo "     the format is 'config_file_path1,hadoop_input_dir1 config_file_path2,hadoop_input_dir2 ...'"
	echo "     the format can be 'config_file_path1 config_file_path2 ...'"
	echo "     the config list file also support the two formats"
	echo "  -g GATEWAY_HOST_FILE  specify gateway host file"
	echo "  -i GATEWAY_INDEX      gateway index"
	echo "  -d DEAD_GATEWAY_INDEX dead gateway index"
	echo "  -s DEAD_GATEWAY_SERVER dead gateway server ip"
	echo "  -n GATEWAY_COUNT      gateway count"
	echo "  -m                    dispatch sstable to chunkserver with multiple gateway"
	echo "  -b                    map reduce build sstable in hadoop"
	echo "  -f                    major freeze the only one OB cluster in config file"
	echo "  -o CHUNK_SERVER_IP    each gateway only copy the sstable belong to the specified chunk server"
}

check_param()
{
	# if set -m option, must specify gateway hosts
	if [ "X$MULT_DISPATCH" != "X" ]; then
		if [ "X$GATEWAY_HOSTS" == "X" -a "X$GATEWAY_HOST_FILE" == "X" ]; then
			echo "ERROR: not specify gateway hosts"
			usage
			exit 2
		fi
	elif [ "X$MR_GEN_SSTABLE" == "X" ]; then 
		if [ "X$MAJOR_FREEZE" != "X" ]; then
			return 0
		fi

		if [ "X$GATEWAY_INDEX" == "X" ]; then
			echo "ERROR: not specify gateway index"
			usage
			exit 2
		fi

		if [ "X$GATEWAY_COUNT" == "X" ]; then
			echo "ERROR: not specify gateway count"
			usage
			exit 2
		fi

		if [ $GATEWAY_INDEX -ge $GATEWAY_COUNT ]; then
			echo "ERROR: GATEWAY_INDEX=$GATEWAY_INDEX is greater or equal to GATEWAY_COUNT=$GATEWAY_COUNT"
			usage
			exit 2
		fi

		if [ "X$DEAD_GATEWAY_INDEX" != "X" ]; then
			if [ $DEAD_GATEWAY_INDEX -ge $GATEWAY_COUNT ]; then
				echo "ERROR: DEAD_GATEWAY_INDEX=$DEAD_GATEWAY_INDEX is greater or equal to GATEWAY_COUNT=$GATEWAY_COUNT"
				usage
				exit 2
			fi
			if [ "X$DEAD_GATEWAY_SERVER" == "X" ]; then
				echo "ERROR: DEAD_GATEWAY_SERVER not specify, but specify DEAD_GATEWAY_INDEX=$DEAD_GATEWAY_INDEX"
				usage
				exit 2
			fi
		else
			if [ "X$DEAD_GATEWAY_SERVER" != "X" ]; then
				echo "ERROR: DEAD_GATEWAY_INDEX not specify, but specify DEAD_GATEWAY_SERVER=$DEAD_GATEWAY_SERVER"
				usage
				exit 2
			fi
		fi

		if [ "X$DEAD_GATEWAY_INDEX" != "X" -a "X$ONLY_COPY_SERVER" != "X" ]; then
			echo "ERROR: not support specify DEAD_GATEWAY_INDEX and ONLY_COPY_SERVER at the same time"
			usage
			exit 2
		fi
	fi
}

parse_config_file_param()
{
	# must specify config file for each table to dispatch
	ORG_CONFIG_FILES=()
	CONFIG_FILES=()
	INPUT_DIRS=()
	if [ "X$TABLE_CONFIG_FILES" == "X" -a "X$CONFIG_LIST_FILE" == "X" ]; then
		echo "ERROR: not specify table config files"
		usage
		exit 2
	else
		declare -i i=0
		if [ "X$TABLE_CONFIG_FILES" != "X" ]; then
			for file in $TABLE_CONFIG_FILES
			do
				declare -i match_len=$(expr match $file ".*,.*")
				if [ $match_len -eq ${#file} ]; then
					CONFIG_FILES[$i]=${file%%,*}
					INPUT_DIRS[$i]=${file#*,}
				else
					CONFIG_FILES[$i]=$file
				fi
				ORG_CONFIG_FILES[$i]=$file
				i=$((i+1))
			done
		else
			if [ ! -f $CONFIG_LIST_FILE ]; then
				echo "ERROR: config list file $CONFIG_LIST_FILE is not exist!"
				exit 2
			fi
			while read file; do
				declare -i match_len=$(expr match $file ".*,.*")
				if [ $match_len -eq ${#file} ]; then
					CONFIG_FILES[$i]=${file%%,*}
					INPUT_DIRS[$i]=${file#*,}
				else
					CONFIG_FILES[$i]=$file
				fi
				ORG_CONFIG_FILES[$i]=$file
				i=$((i+1))
			done < $CONFIG_LIST_FILE
		fi

		if [ ${#CONFIG_FILES[@]} -eq 0 ]; then
			echo "ERROR: there is no table config file, config_file_count=${#CONFIG_FILES[@]}"
			usage
			exit 2
		else
			for file in ${CONFIG_FILES[@]}
			do
				if [ ! -f $file ]; then
					echo "ERROR: config file $file is not exist!"
					usage
					exit 2
				fi
			done
		fi

		if [ ${#INPUT_DIRS[@]} -gt 0 -a ${#CONFIG_FILES[@]} -ne ${#INPUT_DIRS[@]} ]; then
			echo "ERROR: config file count doesn't equal input dirs, config_file_count=${#CONFIG_FILES[@]}, input_dir_count=${#INPUT_DIRS[@]}"
			usage
			exit 2
		fi
	fi
}

parse_gateway_host_param()
{
	# parse gateway hosts to array
	GW_HOSTS=()
	if [ "X$MULT_DISPATCH" != "X" ]; then 
		declare -i i=0
		if [ "X$GATEWAY_HOSTS" != "X" ]; then
			for host in $GATEWAY_HOSTS
			do
				GW_HOSTS[$i]=$host
				i=$((i+1))
			done
		else
			if [ ! -f $GATEWAY_HOST_FILE ]; then
				echo "ERROR: gateway host file $GATEWAY_HOST_FILE is not exist!"
				exit 2
			fi
			while read host; do
				GW_HOSTS[$i]=$host
				i=$((i+1))
			done < $GATEWAY_HOST_FILE
		fi

		if [ ${#GW_HOSTS[@]} -eq 0 ]; then
			echo "ERROR: there is no gateway host, host_count=${#GW_HOSTS[@]}"
			usage
			exit 2
		fi
		GATEWAY_COUNT=${#GW_HOSTS[@]}
	fi
}

# get a configuration option from config file 
# get_conf <config file> <param> <default value>
get_conf() 
{
	declare var=$(grep "^$2=" $1)
		if [ "X$var" != "X" ]; then
			echo "$var"| head -n 1 | awk -F"=" '{print "1 "$2}'| awk '{print $2}'
		else
			echo "$3";
	fi
}

# print log function
mlog() 
{
	declare msg=$1;
	declare time=$(date  "+%Y-%m-%d %T");
	echo "[$time]     [$GATEWAY_INDEX] $msg" >> $2;
}

errorlog()
{
	touch $FAILURE_FILE
	echo "[$GATEWAY_INDEX] $1" > $FAILURE_FILE
	echo "[$GATEWAY_INDEX] $1"
	mlog "$1" $LOG_FILE
}

infolog()
{
	echo "[$GATEWAY_INDEX] $1"
	mlog "$1" $LOG_FILE
}

# read param from config file
# read_param_from_config_file <config_file>
read_param_from_config_file()
{
	declare conf=$1

	#read parameter owned by table
	APP_CONFIG_FILE=$(get_conf "$conf" "APP_CONFIG_FILE" "")
	HADOOP_INPUT_DIR=$(get_conf "$conf" "HADOOP_INPUT_DIR" "/home/input/")
	DATA_DAYS_AGO=$(get_conf "$conf" "DATA_DAYS_AGO" "")
	TABLE_NAME=$(get_conf "$conf" "TABLE_NAME" "table_name")
	TABLE_ID=$(get_conf "$conf" "TABLE_ID" "1001")
	ROWKEY_SPLIT=$(get_conf "$conf" "ROWKEY_SPLIT" "3-8")
	ROWKEY_DELIM_ASCII=$(get_conf "$conf" "ROWKEY_DELIM_ASCII" "")

	#if user specify config file of app, read it
	if [ "X$APP_CONFIG_FILE" != "X" ]; then
		conf=$APP_CONFIG_FILE
	fi
	HADOOP_BIN_DIR=$(get_conf "$conf" "HADOOP_BIN_DIR" "/home/hadoop/hadoop-current/bin/")
	HADOOP_HDFS_PATH=$(get_conf "$conf" "HADOOP_HDFS_PATH" "hdfs://hdpnn:9000")
	HADOOP_BIN_CONFIG_DIR=$(get_conf "$conf" "HADOOP_BIN_CONFIG_DIR" "")
	HADOOP_DATA_DIR_TOP=$(get_conf "$conf" "HADOOP_DATA_DIR" "/home/output/")
	HADOOP_CONFIG_DIR=$(get_conf "$conf" "HADOOP_CONFIG_DIR" "/home/config/")

	DATA_LOCAL_DIR=$(get_conf "$conf" "DATA_LOCAL_DIR" "/home/admin/data_dispatcher/data/")
	DST_DATA_DIR=$(get_conf "$conf" "DST_DATA_DIR" "/data/")
	APP_NAME=$(get_conf "$conf" "APP_NAME" "app_name")
	DISK_COUNT=$(get_conf "$conf" "DISK_COUNT" "10")
	USER_NAME=$(get_conf "$conf" "USER_NAME" "admin")

	SCRIPTS_DIR=$(get_conf "$conf" "SCRIPTS_DIR" "/home/admin/data_dispatcher/")
	BIN_DIR=$(get_conf "$conf" "BIN_DIR" "/home/admin/data_dispatcher/bin/")
	ETC_DIR=$(get_conf "$conf" "ETC_DIR" "/home/admin/data_dispatcher/etc/")
	LOG_DIR=$(get_conf "$conf" "LOG_DIR" "/home/admin/data_dispatcher/log/")
	if [ "X$DATA_DAYS_AGO" == "X" ]; then
		DATA_DAYS_AGO=$(get_conf "$conf" "DATA_DAYS_AGO" "0")
	fi
	if [ "X$ROWKEY_DELIM_ASCII" == "X" ]; then
		ROWKEY_DELIM_ASCII=$(get_conf "$conf" "ROWKEY_DELIM_ASCII" "1")
	fi
	RM_DAYS_AGO=$(get_conf "$conf" "RM_DAYS_AGO" "3")

	ROOTSERVER_IP=$(get_conf "$conf" "ROOTSERVER_IP" "")
	ROOTSERVER_PORT=$(get_conf "$conf" "ROOTSERVER_PORT" "2500")

	UPDATESERVER_IP=$(get_conf "$conf" "UPDATESERVER_IP" "")
	UPDATESERVER_PORT=$(get_conf "$conf" "UPDATESERVER_PORT" "2900")
	CTL_TABLE_NAME=$(get_conf "$conf" "CTL_TABLE_NAME" "ctl_table")
	MAJOR_FREEZE_INTERVAL_SECOND=$(get_conf "$conf" "MAJOR_FREEZE_INTERVAL_SECOND" "3600")
}

# init global variables from config file
global_var_init()
{
	declare log_date=$(date +"%Y%m%d")
	declare pathdate=$(date -d "$DATA_DAYS_AGO day ago" +"%Y-%m-%d")
	HADOOP_DATA_DIR=$HADOOP_DATA_DIR_TOP$APP_NAME/$pathdate/$TABLE_NAME/
	LOCAL_TABLE_DIR=$DATA_LOCAL_DIR$APP_NAME/$pathdate/$TABLE_NAME
	FAILURE_FILE="$LOCAL_TABLE_DIR/$APP_NAME-$TABLE_NAME.fail"
	SUCCESS_FILE="$LOCAL_TABLE_DIR/$APP_NAME-$TABLE_NAME.done.$log_date"
	SUCC_MR_GEN_SSTABLE_FLAG_FILE="$LOCAL_TABLE_DIR/mr_gen_sstable.done"
	FAIL_MR_GEN_SSTABLE_FLAG_FILE="$LOCAL_TABLE_DIR/mr_gen_sstable.fail"
	FINISH_HADOOP_COPY_FLAG_FILE="$LOCAL_TABLE_DIR/data_copy.done"
	FINISH_DISP_FLAG_FILE="$LOCAL_TABLE_DIR/data_dispatched.done" 
	FINISH_UPS_FREEZE="$LOCAL_TABLE_DIR/ups_freeze.done"
	DATA_TMP_DIR="$LOCAL_TABLE_DIR/tmp/"
	DATA_WORK_DIR="$LOCAL_TABLE_DIR/work/"
	DATA_DISPATCHED_DIR="$LOCAL_TABLE_DIR/dispatched/"
	DATA_FAIL_DIR="$LOCAL_TABLE_DIR/fail/"
	LOCK_FILE="$LOCAL_TABLE_DIR/data_dispatch.lock"
	LOG_FILE="$LOG_DIR$APP_NAME/$TABLE_NAME/data_dispatch.log.$log_date"
	RANGE_FILE="$LOCAL_TABLE_DIR/$APP_NAME-$TABLE_NAME-$TABLE_ID-range.ini"
	RANGE_FILE_OLD="$LOCAL_TABLE_DIR/$APP_NAME-$TABLE_NAME-$TABLE_ID-range-old.ini"

	APP_ETC_DIR=$ETC_DIR$APP_NAME/
	TABLE_ETC_DIR=$ETC_DIR$APP_NAME/$TABLE_NAME/
	TABLE_CONFIG_DIR=$HADOOP_CONFIG_DIR$APP_NAME/$TABLE_NAME/
	APP_FREEZE_TIME_FILE_PREFIX="$DATA_LOCAL_DIR$APP_NAME/freeze_time-"

	if [ "X$HADOOP_BIN_CONFIG_DIR" != "X" ]; then
		HADOOP_BIN_CONFIG_PARAM="--config $HADOOP_BIN_CONFIG_DIR"
	fi
	
	if [ "X$GATEWAY_INDEX" == "X" ]; then
		GATEWAY_INDEX=0
	fi

	if [ "X$GATEWAY_COUNT" == "X" ]; then
		GATEWAY_COUNT=1
	fi

	ORG_FILES=()
	DISP_FILES=()
	CS_SERVER1=()
	CS_SERVER2=()
	CS_SERVER3=()

	HADOOP_GET_SUCC_COUNT=0
	HADOOP_GET_FAIL_COUNT=0
	HADOOP_GET_FILE_TOTAL_COUNT=0
	SCP_FILE_SUCC_COUNT=0
	SCP_FILE_FAIL_COUNT=0
	SCP_FILE_TOTAL_COUNT=0
	GATEWAY_FAIL_COUNT=0
	GATEWAY_SUCC_COUNT=0
}

#parse command line param, hadoop input dir list
# parse_cli_param <sequence_no>
parse_cli_param()
{
	if [ ${#INPUT_DIRS[@]} -gt 0 ]; then
		HADOOP_INPUT_DIR=${INPUT_DIRS[$1]}
	fi
}

make_data_dir()
{
	# make log dir first, otherwise we can't print log
	declare log_dir=$LOG_DIR$APP_NAME/$TABLE_NAME/
	if [ ! -e $log_dir ]; then
		mkdir -p $log_dir
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: make dispatched dir failed, $log_dir"
			exit 2
		fi
	fi

	# make data directory for current application and table
	mlog "INFO: make data directory ......" $LOG_FILE
	if [ ! -e $DATA_TMP_DIR ]; then
		mkdir -p $DATA_TMP_DIR
		if [ "X$?" != "X0" ]; then
			 errorlog "ERROR: make tmp dir failed, $DATA_TMP_DIR"
				exit 2
		fi
	fi

	if [ ! -e $DATA_WORK_DIR ]; then
		mkdir -p $DATA_WORK_DIR
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: make work dir failed, $DATA_WORK_DIR"
			exit 2
		fi
	fi

	if [ ! -e $DATA_DISPATCHED_DIR ]; then
		mkdir -p $DATA_DISPATCHED_DIR
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: make dispatched dir failed, $DATA_DISPATCHED_DIR"
			exit 2
		fi
	fi

	if [ ! -e $DATA_FAIL_DIR ]; then
		mkdir -p $DATA_FAIL_DIR
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: make fail dir failed, $DATA_FAIL_DIR"
			exit 2
		fi
	fi

	# make etc directory for current application and table
	if [ ! -e $TABLE_ETC_DIR ]; then
		mkdir -p $TABLE_ETC_DIR
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: make table etc dir failed, $TABLE_ETC_DIR"
			exit 2
		fi
	fi
}

check_dir_format()
{
	# all the directories must end with /, this binary dependent it
	if [ "X${HADOOP_BIN_DIR: -1}" != "X/" ]; then
		HADOOP_BIN_DIR=$HADOOP_BIN_DIR/
	fi

	if [ "X$HADOOP_BIN_CONFIG_DIR" != "X" -a "X${HADOOP_BIN_CONFIG_DIR: -1}" != "X/" ]; then
		HADOOP_BIN_CONFIG_DIR=$HADOOP_BIN_CONFIG_DIR/
	fi

	if [ "X${HADOOP_INPUT_DIR: -1}" != "X/" ]; then
		HADOOP_INPUT_DIR=$HADOOP_INPUT_DIR/ 
	fi

	if [ "X${HADOOP_DATA_DIR: -1}" != "X/" ]; then
		HADOOP_DATA_DIR=$HADOOP_DATA_DIR/
	fi

	if [ "X${HADOOP_CONFIG_DIR: -1}" != "X/" ]; then
		HADOOP_CONFIG_DIR=$HADOOP_CONFIG_DIR/
	fi

	if [ "X${DATA_LOCAL_DIR: -1}" != "X/" ]; then
		DATA_LOCAL_DIR=$DATA_LOCAL_DIR/
	fi

	if [ "X${DST_DATA_DIR: -1}" != "X/" ]; then
		DST_DATA_DIR=$DST_DATA_DIR/
	fi

	if [ "X${SCRIPTS_DIR: -1}" != "X/" ]; then
		SCRIPTS_DIR=$SCRIPTS_DIR/
	fi

	if [ "X${ETC_DIR: -1}" != "X/" ]; then
		ETC_DIR=$ETC_DIR/
	fi

	if [ "X${BIN_DIR: -1}" != "X/" ]; then
		BIN_DIR=$BIN_DIR/
	fi

	if [ "X${LOG_DIR: -1}" != "X/" ]; then
		LOG_DIR=$LOG_DIR/
	fi
}

check_config_param()
{
	# gateway count and disk count must be greater than 0, we use it do mod and divide operation
	if [ "X$MR_GEN_SSTABLE" == "X" -a "X$MAJOR_FREEZE" == "X" ]; then
		if [ $GATEWAY_COUNT -le 0 ]; then
			errorlog "ERROR: GATEWAY_COUNT must be greater than 0, GATEWAY_COUNT=$GAGEWAY_COUNT"
			exit 2
		fi

		if [ $DISK_COUNT -le 0 ]; then
			errorlog "ERROR: DISK_COUNT must be greater than 0, DISK_COUNT=$DISK_COUNT"
			exit 2
		fi
	fi

	if [ $RM_DAYS_AGO -le $DATA_DAYS_AGO ]; then
		errorlog "ERROR: DATA_DAYS_AGO must be greater than RM_DAYS_AGO, DATA_DAYS_AGO=$DATA_DAYS_AGO, RM_DAYS_AGO=$RM_DAYS_AGO"
		exit 2
	fi
}

# shell reentry.
check_reentry()
{
	# for each application and table, only run one process at the same time 
	mlog "INFO: get lockfile=$LOCK_FILE ......" $LOG_FILE
	if [ -f $LOCK_FILE ];then
		infolog "INFO: $0 re-entry, $LOCK_FILE exist!!"
		exit 1
	fi
	touch $LOCK_FILE

	if [ -f $FINISH_DISP_FLAG_FILE ]; then
		infolog "INFO: $FINISH_DISP_FLAG_FILE exist, needn't dispatch again" 
		rm -rf $LOCK_FILE
		exit 1
	fi
}

# map reduce generate sstable in hadoop
mr_generate_sstable()
{
	infolog "INFO: start map reduce generate sstable"

	# if this table of current application is done, just return
	declare succ_file=$SUCC_MR_GEN_SSTABLE_FLAG_FILE
	declare file_file=$FAIL_MR_GEN_SSTABLE_FLAG_FILE
	if [ -f $succ_file ]; then
		infolog "INFO: $succ_file exist, needn't map reduce sstable again"
		return 0
	fi
	if [ -f $fail_file ]; then
		rm -rf $fail_file
	fi

	# check parameter 
	declare hadoop_bin="${HADOOP_BIN_DIR}hadoop"
	if [ ! -e $hadoop_bin ]; then
		errorlog "ERROR: $hadoop_bin isn't existent"
		return 2
	fi

	declare mrsstable_bin="${BIN_DIR}mrsstable.jar"
	if [ ! -e $mrsstable_bin ]; then
		errorlog "ERROR: $mrsstable_bin isn't existent"
		return 2
	fi

	declare mr_configuration="${TABLE_ETC_DIR}configuration.xml"
	if [ ! -e $mr_configuration ]; then
		errorlog "ERROR: $mr_configuration isn't existent"
		return 2
	fi

	declare data_syntax="${TABLE_ETC_DIR}data_syntax.ini"
	if [ ! -e $data_syntax ]; then
		errorlog "ERROR: $data_syntax isn't existent"
		return 2
	fi

	if [ -f ${APP_ETC_DIR}schema.ini ]; then
		cp ${APP_ETC_DIR}schema.ini ${TABLE_ETC_DIR}schema.ini
	fi
	if [ ! -e ${TABLE_ETC_DIR}schema.ini ]; then
		errorlog "ERROR: ${TABLE_ETC_DIR}schema.ini isn't existent"
		return 2
	fi
	
	# delete config directory and remake it in hadoop, then remove output directory,
	# next jar the config files and put the config jar file into the config 
	# directory in hadoop
	$hadoop_bin $HADOOP_BIN_CONFIG_PARAM fs -rmr $TABLE_CONFIG_DIR
	$hadoop_bin $HADOOP_BIN_CONFIG_PARAM fs -mkdir $TABLE_CONFIG_DIR
	$hadoop_bin $HADOOP_BIN_CONFIG_PARAM fs -rmr $HADOOP_DATA_DIR

	if [ "X$ROOTSERVER_IP" != "X" ]; then
		if [ -e $RANGE_FILE ]; then
			cp $RANGE_FILE ${TABLE_ETC_DIR}range.lst
		else
			errorlog "ERROR: $RANGE_FILE isn't existent"
			return 2
		fi
	else
		if [ -e ${TABLE_ETC_DIR}range.lst ]; then
			touch ${TABLE_ETC_DIR}range.lst
		fi
	fi	

	declare config_jar="config.jar"
	cd $TABLE_ETC_DIR && jar cvf $config_jar schema.ini data_syntax.ini range.lst
	if [ "X$?" != "X0" ]; then
		errorlog "ERROR: failed to jar config file $TABLE_ETC_DIR$config_jar"
		return 2
	fi

	$hadoop_bin $HADOOP_BIN_CONFIG_PARAM fs -put $TABLE_ETC_DIR$config_jar $TABLE_CONFIG_DIR
	if [ "X$?" != "X0" ]; then
		errorlog "ERROR: failed to put config file $TABLE_ETC_DIR$config_jar to $TABLE_CONFIG_DIR"
		return 2
	fi

	# run the actual command in hadoop to generate sstable for current table and application
	$hadoop_bin $HADOOP_BIN_CONFIG_PARAM jar $mrsstable_bin com.taobao.mrsstable.MRGenSstable -conf $mr_configuration -archives $HADOOP_HDFS_PATH$TABLE_CONFIG_DIR$config_jar $HADOOP_INPUT_DIR $HADOOP_DATA_DIR
	if [ "X$?" != "X0" ]; then
		errorlog "ERROR: failed to map reduce generate sstable"
		return 2
	fi

	touch $succ_file
	infolog "INFO: map reduce generate sstable done"
}

# build range file acorrding to the current application and table
# it will scan the root table inf OceanBase rootserver, and transfer
# the root table of current table into specifical format, then 
# hadoop can use this range file to generate sstable, and this process
# can use it dispatch sstable to chunkserver
build_range_file()
{
	mlog "INFO: start build rowkey range file" $LOG_FILE
	if [ -s $RANGE_FILE ]; then
		mlog "INFO: $RANGE_FILE is existent, needn't build again" $LOG_FILE
		echo "INFO: $RANGE_FILE is existent, needn't build again"
		return 0
	fi

	if [ "X$ROOTSERVER_IP" == "X" ]; then
		mlog "INFO: not specify rooserver ip, needn't scan root table to build range file: $RANGE_FILE" $LOG_FILE
		echo "INFO: not specify rooserver ip, needn't scan root table to build range file: $RANGE_FILE"
		return 0
	fi

	#use rs_admin tool to disable balance
#	declare rs_admin_file="${BIN_DIR}rs_admin"
#	if [ ! -f $rs_admin_file ]; then
#		errorlog "ERROR: $rs_admin_file isn't existent"
#		exit 2
#	fi
#	$rs_admin_file -r $ROOTSERVER_IP -p $ROOTSERVER_PORT disable_balance
#	if [ "X$?" != "X0" ]; then
#		errorlog "ERROR: failed to send disable balance command to rootserver"
#		exit 2
#	fi
#	mlog "INFO: after send disable balance command to rootserver, sleep 30s" $LOG_FILE
#	echo "INFO: after send disable balance command to rootserver, sleep 30s"
#	sleep 30

	# use cs_admin tool to create range file
	declare cs_admin_file="${BIN_DIR}cs_admin"
	if [ ! -f $cs_admin_file ]; then
		errorlog "ERROR: $cs_admin_file isn't existent"
		exit 2
	fi
	$cs_admin_file -s $ROOTSERVER_IP -p $ROOTSERVER_PORT -q -i "scan_root_table $TABLE_NAME $TABLE_ID $ROWKEY_SPLIT $APP_NAME $LOCAL_TABLE_DIR $ROWKEY_DELIM_ASCII"
	if [ "X$?" != "X0" ]; then
		errorlog "ERROR: failed to build rowkey range file $RANGE_FILE"
		exit 2
	fi

	if [ ! -f $RANGE_FILE ]; then
		errorlog "ERROR: after build range file, $RANGE_FILE isn't existent"
		exit 2
	fi
	infolog "INFO: finish building rowkey range file"
}

# parse range file, get the mapping of hadoop file to chunkserver
build_dispatch_map()
{
	mlog "INFO: start build dispatch map" $LOG_FILE
	
	# get sstable files list in hadoop
	ORG_FILES=($(${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -ls $HADOOP_DATA_DIR | egrep -v "Found|_logs" | awk -F ' ' '{print $8}'))
	if [ ${#ORG_FILES[@]} -le 0 ]; then
		errorlog "ERROR: no hadoop file to copy"
		exit 2
	fi

	# the hadoop sstable file name format: part-00001-1001-000001
	# 	part    is fix prefix
	#   00001   5 chars, partition number
	#   1001    table id
	#   000001  6 chars, tablet range number
	#
	# range file line format:
	#   1999^A-1 1001-000000 00000000000007CFFFFFFFFFFFFFFFFF 10.232.36.39 10.232.36.38
	#     1999^A-1  rowkey column separate by special delimeter, ex: \1
	#     1001-000001 table id and tablet range number
	#     00000000000007CFFFFFFFFFFFFFFFFF binary rowkey for hex format
	#     10.232.36.39  the first chunkserver ip
	#     10.232.36.38  the second chunkserver ip
	#     ...           the third chunkserver ip if it exist
	declare -i org_file_index=0
	declare -i disp_file_index=0
	declare prev_file_prefix
	declare name_part
	declare file
	declare old_ifs=$IFS
	export IFS=' '
	while read -a array; do
		if [ $org_file_index -lt ${#ORG_FILES[@]} ]; then
			file=${ORG_FILES[$org_file_index]#${HADOOP_DATA_DIR}}
			prev_file_prefix=${file%-[0-9]*-[0-9][0-9][0-9][0-9][0-9][0-9]}
			declare -i match_len=$(expr match $file "part-[0-9][0-9][0-9][0-9][0-9]-[0-9]*-[0-9][0-9][0-9][0-9][0-9][0-9]")
			if [ $match_len -ne ${#file} ]; then
				errorlog "ERROR: the format of hadoop file name is invalid, ${ORG_FILES[$org_file_index]}"
				continue
			fi

			name_part=${file#part-[0-9][0-9][0-9][0-9][0-9]-}
		fi

		if [ "X$name_part" == "X${array[1]}" ]; then
			declare -i num=$(echo ${file: -6} | sed 's/^0\{1,5\}//g')
			if [ $num -ne $disp_file_index ]; then
				errorlog "ERROR: hadoop file index %num isn't the same as expected index %disp_file_index"
				exit 2	
			else
				DISP_FILES[$num]=$file
				org_file_index=$((org_file_index+1))
			fi
		else
			DISP_FILES[$disp_file_index]=$prev_file_prefix-${array[1]}
		fi
			
		if [ "X${array[3]}" != "X" ]
		then
			CS_SERVER1[$disp_file_index]=${array[3]}
		fi

		if [ "X${array[4]}" != "X" ]
		then
			CS_SERVER2[$disp_file_index]=${array[4]}
		fi

		if [ "X${array[5]}" != "X" ]
		then
			CS_SERVER3[$disp_file_index]=${array[5]}
		fi
		disp_file_index=$((disp_file_index+1))
	done < $RANGE_FILE
	export IFS=$old_ifs
	
	if [ $org_file_index -ne ${#ORG_FILES[@]} ]; then
		errorlog "ERROR: wrong files count, actual_files_count=$org_file_index, expected_files_count=${#ORG_FILES[@]}"
		exit 2	
	fi

	infolog "INFO: build_dispatch_map done, actual_files_count=$org_file_index, expected_files_count=${#ORG_FILES[@]}"
}

# copy sstable files from hadoop, each gateway only part of the sstable files
copy_data_from_hadoop()
{
	infolog "INFO: starting copy data from hadoop ......"
	declare -i gateway_index=$GATEWAY_INDEX
	if [ "X$DEAD_GATEWAY_INDEX" != "X" ]; then
		gateway_index=$DEAD_GATEWAY_INDEX
	fi
	declare -i total_file_count=${#DISP_FILES[@]}
	declare -i step_size=$((total_file_count/GATEWAY_COUNT))
	declare -i start_index=$((gateway_index*step_size))
	declare -i copy_count=$step_size
	if [ $gateway_index -eq $((GATEWAY_COUNT-1)) ]; then
		copy_count=$((total_file_count%GATEWAY_COUNT+step_size))
	fi

	if [ "X$DEAD_GATEWAY_INDEX" != "X" ]; then
		total_file_count=$copy_count
		step_size=$((total_file_count/(GATEWAY_COUNT-1)))
		if [ $GATEWAY_INDEX -gt $DEAD_GATEWAY_INDEX ]; then
			start_index=$((start_index+(GATEWAY_INDEX-1)*step_size))		
		else
			start_index=$((start_index+GATEWAY_INDEX*step_size))		
		fi		
		copy_count=$step_size
		if [ $GATEWAY_INDEX -eq $((GATEWAY_COUNT-1)) ]; then
			copy_count=$((total_file_count%(GATEWAY_COUNT-1)+step_size))
		elif [ $DEAD_GATEWAY_INDEX -eq $((GATEWAY_COUNT-1)) -a $GATEWAY_INDEX -eq $((GATEWAY_COUNT-2))]; then
			copy_count=$((total_file_count%(GATEWAY_COUNT-1)+step_size))
		fi
	fi
	HADOOP_GET_FILE_TOTAL_COUNT=$copy_count
	
	if [ "X$ONLY_COPY_SERVER" != "X" ]; then
		start_index=0
		copy_count=${#DISP_FILES[@]}	
		HADOOP_GET_FILE_TOTAL_COUNT=0
	fi
	declare -i end_index=$((start_index+copy_count))

	rm -rf ${DATA_TMP_DIR}*
	rm -rf ${DATA_WORK_DIR}*
	declare -a files=()
	declare -i files_count=0
	declare -a files_index=()
	declare -a fail_files=()
	declare -i fail_files_count=0
	declare -a fail_files_index=()
	for ((i=$start_index;i<$end_index;++i))
	do
		declare file_name=${DISP_FILES[$i]}
		declare -i num=$(echo ${file_name: -6} | sed 's/^0\{1,5\}//g')
		if [ "X$ONLY_COPY_SERVER" != "X" ]; then
			if [ "X${CS_SERVER1[$num]}" != "X" -a "X${CS_SERVER1[$num]}" == "X$ONLY_COPY_SERVER" ]; then
				HADOOP_GET_FILE_TOTAL_COUNT=$((HADOOP_GET_FILE_TOTAL_COUNT+1))
			elif [ "X${CS_SERVER2[$num]}" != "X" -a "X${CS_SERVER2[$num]}" == "X$ONLY_COPY_SERVER" ]; then
				HADOOP_GET_FILE_TOTAL_COUNT=$((HADOOP_GET_FILE_TOTAL_COUNT+1))
			elif [ "X${CS_SERVER3[$num]}" != "X" -a "X${CS_SERVER3[$num]}" == "X$ONLY_COPY_SERVER" ]; then
				HADOOP_GET_FILE_TOTAL_COUNT=$((HADOOP_GET_FILE_TOTAL_COUNT+1))
			else
				#the sstable doesn't belong to the same chunk server of gateway, skip it
				continue
			fi
		fi

		${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -test -e ${HADOOP_DATA_DIR}$file_name
		if [ $? -ne 0 ]; then
 			#empty sstable file, just touch one empty sstable file, not copy from hadoop
			infolog "INFO: touch empty sstable file: ${DATA_TMP_DIR}${file_name}" 
			touch ${DATA_TMP_DIR}$file_name
			if [ -f ${DATA_TMP_DIR}${DISP_FILES[$num]} ]; then
				mlog "INFO: mv local file: ${DATA_TMP_DIR}${DISP_FILES[$num]} to ${DATA_WORK_DIR}${DISP_FILES[$num]}" $LOG_FILE
				mv ${DATA_TMP_DIR}${DISP_FILES[$num]} ${DATA_WORK_DIR}${DISP_FILES[$num]}
				HADOOP_GET_SUCC_COUNT=$((HADOOP_GET_SUCC_COUNT+1))
			else
				errorlog "ERORR: file ${DATA_TMP_DIR}${DISP_FILES[$num]} isn't existent, can't move"
				HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
			fi
		else 
			files[$files_count]=${HADOOP_DATA_DIR}$file_name
			files_index[$files_count]=$num
			files_count=$((files_count+1))
		fi

		if [ $files_count -ge 2 ]; then
			for ((j=0;j<$files_count;j++))
			do
				num=${files_index[$j]}
				${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -get ${files[$j]} ${DATA_TMP_DIR}${DISP_FILES[$num]} &
			done

			for ((j=0;j<$files_count;j++))
			do
				k=$((j+1))
				num=${files_index[$j]}
				wait %$k			
				if [ "X$?" != "X0" ]; then
					errorlog "ERROR: failed to get file ${files[$j]} from hadoop to ${DATA_TMP_DIR}${DISP_FILES[$num]}"
					HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
					fail_files[$fail_files_count]=${files[$j]}
					fail_files_index[$fail_files_count]=$num
					fail_files_count=$((fail_files_count+1))
					continue
				else
					infolog "INFO: get hadoop file: ${files[$j]} from hadoop to dir: ${DATA_TMP_DIR}${DISP_FILES[$num]}"
				fi

				if [ -f ${DATA_TMP_DIR}${DISP_FILES[$num]} ]; then
					mlog "INFO: mv local file: ${DATA_TMP_DIR}${DISP_FILES[$num]} to ${DATA_WORK_DIR}${DISP_FILES[$num]}" $LOG_FILE
					mv ${DATA_TMP_DIR}${DISP_FILES[$num]} ${DATA_WORK_DIR}${DISP_FILES[$num]}
					HADOOP_GET_SUCC_COUNT=$((HADOOP_GET_SUCC_COUNT+1))
				else
					errorlog "ERORR: file ${DATA_TMP_DIR}${DISP_FILES[$num]} isn't existent, can't move"
					HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
				fi
			done
			files=()
			files_index=()
			files_count=0
		fi
	done

	if [ $files_count -ge 0 ]; then
		for ((j=0;j<$files_count;j++))
		do
			num=${files_index[$j]}
			${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -get ${files[$j]} ${DATA_TMP_DIR}${DISP_FILES[$num]} &
		done

		for ((j=0;j<$files_count;j++))
		do
			k=$((j+1))
			num=${files_index[$j]}
			wait %$k			
			if [ "X$?" != "X0" ]; then
				errorlog "ERROR: failed to get file ${files[$j]} from hadoop to ${DATA_TMP_DIR}${DISP_FILES[$num]}"
				HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
				fail_files[$fail_files_count]=${files[$j]}
				fail_files_index[$fail_files_count]=$num
				fail_files_count=$((fail_files_count+1))
				continue
			else
				infolog "INFO: get hadoop file: ${files[$j]} from hadoop to dir: ${DATA_TMP_DIR}${DISP_FILES[$num]}"
			fi

			if [ -f ${DATA_TMP_DIR}${DISP_FILES[$num]} ]; then
				mlog "INFO: mv local file: ${DATA_TMP_DIR}${DISP_FILES[$num]} to ${DATA_WORK_DIR}${DISP_FILES[$num]}" $LOG_FILE
				mv ${DATA_TMP_DIR}${DISP_FILES[$num]} ${DATA_WORK_DIR}${DISP_FILES[$num]}
				HADOOP_GET_SUCC_COUNT=$((HADOOP_GET_SUCC_COUNT+1))
			else
				errorlog "ERORR: file ${DATA_TMP_DIR}${DISP_FILES[$num]} isn't existent, can't move"
				HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
			fi
		done
		files=()
		files_index=()
		files_count=0
	fi

	#loop to retry get failed sstable from hadoop until it gets successfully
	if [ $fail_files_count -ge 0 ]; then
		for ((j=0;j<$fail_files_count;))
		do
			num=${fail_files_index[$j]}
			${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -get ${fail_files[$j]} ${DATA_TMP_DIR}${DISP_FILES[$num]}
			if [ "X$?" != "X0" ]; then
				errorlog "ERROR: failed to get file ${fail_files[$j]} from hadoop to ${DATA_TMP_DIR}${DISP_FILES[$num]}"
				HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
				continue
			else
				infolog "INFO: get hadoop file: ${fail_files[$j]} from hadoop to dir: ${DATA_TMP_DIR}${DISP_FILES[$num]}"
			fi

			if [ -f ${DATA_TMP_DIR}${DISP_FILES[$num]} ]; then
				mlog "INFO: mv local file: ${DATA_TMP_DIR}${DISP_FILES[$num]} to ${DATA_WORK_DIR}${DISP_FILES[$num]}" $LOG_FILE
				mv ${DATA_TMP_DIR}${DISP_FILES[$num]} ${DATA_WORK_DIR}${DISP_FILES[$num]}
				HADOOP_GET_SUCC_COUNT=$((HADOOP_GET_SUCC_COUNT+1))
			else
				errorlog "ERORR: file ${DATA_TMP_DIR}${DISP_FILES[$num]} isn't existent, can't move"
				HADOOP_GET_FAIL_COUNT=$((HADOOP_GET_FAIL_COUNT+1))
				continue
			fi
			j=$((j+1))
		done
	fi

	if [ $HADOOP_GET_SUCC_COUNT -ne $HADOOP_GET_FILE_TOTAL_COUNT ]; then
		errorlog "ERROR: failed to copy data from hadoop, succ_count=$HADOOP_GET_SUCC_COUNT, total_count=$HADOOP_GET_FILE_TOTAL_COUNT"
	else
		touch $flag_file
	fi
	infolog "INFO: finish copying data from hadoop, succ_count=$HADOOP_GET_SUCC_COUNT, failed_count=$HADOOP_GET_FAIL_COUNT, total_count=$HADOOP_GET_FILE_TOTAL_COUNT"
}

# run a daemon process to copy sstable files from hadoop
# if the done file is existent, needn't copy again, just 
# use the local sstable files
prepare_scp_data()
{
	declare flag_file=${FINISH_HADOOP_COPY_FLAG_FILE}
	if [ -f $flag_file -a "X$DEAD_GATEWAY_INDEX" == "X" ]; then
		infolog "INFO: $flag_file is existent, needn't copy data from hadoop again"
		if [ -d $DATA_TMP_DIR -a "X$(ls $DATA_TMP_DIR)" != "X" ]; then
			mv ${DATA_TMP_DIR}* ${DATA_WORK_DIR}
		fi
		if [ -d $DATA_FAIL_DIR -a "X$(ls $DATA_FAIL_DIR)" != "X" ]; then
			mv ${DATA_FAIL_DIR}* ${DATA_WORK_DIR}
		fi
	else
		if [ -f $flag_file -a "X$DEAD_GATEWAY_INDEX" != "X" ]; then
			rm -rf $flag_file
		fi
		if [ -f $FINISH_DISP_FLAG_FILE -a "X$DEAD_GATEWAY_INDEX" != "X" ]; then
			rm -rf $FINISH_DISP_FLAG_FILE
		fi
		(copy_data_from_hadoop &)
	fi
}

# scp the range file to chunkserver, only scp one range file to each chunkserver
# the range file is stored in the first disk of chunkserver
scp_range_file()
{
	declare -a cs_servers=(${CS_SERVER1[@]} ${CS_SERVER2[@]} ${CS_SERVER3[@]})
	declare -a uniq_cs_servers=($(echo ${cs_servers[@]} | sed -e 's/ /\n/g' | sort | uniq))
	for cs in ${uniq_cs_servers[@]}
	do
		if [ "X$ONLY_COPY_SERVER" != "X" ]; then
			if [ "X$cs" != "X$ONLY_COPY_SERVER" ]; then
				continue
			fi
		elif [ "X$DEAD_GATEWAY_INDEX" != "X" ]; then
			if [ "X$cs" == "X$DEAD_GATEWAY_SERVER" ]; then
				continue
			fi
		else
			if [ $GATEWAY_INDEX -ne 0 ]; then
				continue	
			fi
		fi

		declare dst_dir="${DST_DATA_DIR}1/${APP_NAME}/import/"
		declare dst_uri=${USER_NAME}@$cs:$dst_dir
		scp -oStrictHostKeyChecking=no -c arcfour $RANGE_FILE $dst_uri & 
		wait %1
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: failed to scp file $RANGE_FILE to $dst_uri"	
		else
			infolog "INFO: scp $RANGE_FILE to $dst_uri" $LOG_FILE
		fi
	done
}

# scp sstable local sstable files to chunkserver, one sstable file will scp to 
# multiple chunkserver at the same time, the flow is as follows:
#  1. the daemon process copy sstable files from hadoop to the "tmp" directory,
#     if copy one sstable file success, it mv the sstable file to "work" directory
#  2. the current process check if there are some sstable files in "work" directory,
#     if there are some sstable files in "work" directory, for each sstable file,
#     create several daemon scp process to send sstable file to multiple chunkserver,
#     otherwise the current process just sleep 2 second
#  3. if the daemon process finishes copying sstable file from hadoop, it will 
#	  create the copy.done flag file, if the current process find the flag file,
#     and there is not sstable to scp, the current process break the loop
scp_data_to_cs()
{
	infolog "INFO: starting scp data to chunk server ......"
	declare flag_file=${FINISH_DISP_FLAG_FILE}
	if [ -f $flag_file ]; then
		infolog "INFO: $flag_file is existent, needn't scp data to chunkserver again"
		return 0
	fi

	scp_range_file

	declare -i is_dispatch_succ=1
	declare -i disk_no=$((GATEWAY_INDEX%DISK_COUNT))
	while ((1))
	do
		declare files=($(ls ${DATA_WORK_DIR}))
		if [ ${#files[@]} -eq 0 ]; then
			if [ -f ${FINISH_HADOOP_COPY_FLAG_FILE} ]; then
				break
			else
				sleep 2
			fi
		else
			for file_name in ${files[@]}
			do
				declare -i num=$(echo ${file_name: -6} | sed 's/^0\{1,5\}//g')
				declare file=${DATA_WORK_DIR}${file_name}
				declare -i cur_disk_no=$((disk_no%DISK_COUNT+1))
				declare new_file_name=${file_name#*part-[0-9]*-}
				declare dst_dir="${DST_DATA_DIR}${cur_disk_no}/${APP_NAME}/import/$new_file_name"
				disk_no=$(((disk_no+1)%DISK_COUNT))
				declare -i server_count=0
				declare -i is_scp_succ=1
				declare -a cs_server=()
				if [ "X${CS_SERVER1[$num]}" != "X" ]; then
					if [ "X$DEAD_GATEWAY_INDEX" != "X" -a "X${CS_SERVER1[$num]}" == "X$DEAD_GATEWAY_SERVER" ]; then
						:
						#skip dead gateway
					elif [ "X$ONLY_COPY_SERVER" != "X" -a "X${CS_SERVER1[$num]}" != "X$ONLY_COPY_SERVER" ]; then
						:
						#this chunk server needn't this sstable, skip it	
					else
						declare dst_uri=${USER_NAME}@${CS_SERVER1[$num]}:${dst_dir}
						scp -oStrictHostKeyChecking=no -c arcfour $file $dst_uri & 
						cs_server[$server_count]=${CS_SERVER1[$num]}
						server_count=$((server_count+1))
					fi
				fi
				
				if [ "X${CS_SERVER2[$num]}" != "X" ]; then
					if [ "X$DEAD_GATEWAY_INDEX" != "X" -a "X${CS_SERVER2[$num]}" == "X$DEAD_GATEWAY_SERVER" ]; then
						:
						#skip dead gateway
					elif [ "X$ONLY_COPY_SERVER" != "X" -a "X${CS_SERVER2[$num]}" != "X$ONLY_COPY_SERVER" ]; then
						:
						#this chunk server needn't this sstable, skip it	
					else
						declare dst_uri=${USER_NAME}@${CS_SERVER2[$num]}:${dst_dir}
						scp -oStrictHostKeyChecking=no -c arcfour $file $dst_uri & 
						cs_server[$server_count]=${CS_SERVER2[$num]}
						server_count=$((server_count+1))
					fi
				fi
				
				if [ "X${CS_SERVER3[$num]}" != "X" ]; then
					if [ "X$DEAD_GATEWAY_INDEX" != "X" -a "X${CS_SERVER3[$num]}" == "X$DEAD_GATEWAY_SERVER" ]; then
						:
						#skip dead gateway
					elif [ "X$ONLY_COPY_SERVER" != "X" -a "X${CS_SERVER3[$num]}" != "X$ONLY_COPY_SERVER" ]; then
						:
						#this chunk server needn't this sstable, skip it	
					else
						declare dst_uri=${USER_NAME}@${CS_SERVER3[$num]}:${dst_dir}
						scp -oStrictHostKeyChecking=no -c arcfour $file $dst_uri & 
						cs_server[$server_count]=${CS_SERVER3[$num]}
						server_count=$((server_count+1))
					fi
				fi

				# wait the daemon scp process one by one, we could check the return value
				for ((i=0;i<$server_count;i++))
				do
					j=$((i+1))
					wait %$j
					declare -i ret=$?
					dst_uri=${USER_NAME}@${cs_server[$i]}:${dst_dir}

					if [ "X$ret" != "X0" ]; then
						errorlog "ERROR: failed to scp file $file to $dst_uri"
						SCP_FILE_FAIL_COUNT=$((SCP_FILE_FAIL_COUNT+1))
						is_scp_succ=0
					else
						infolog "INFO: scp $file to $dst_uri" $LOG_FILE
						SCP_FILE_SUCC_COUNT=$((SCP_FILE_SUCC_COUNT+1))
					fi
				done

				if [ $is_scp_succ -eq 1 ]; then 
					mlog "INFO: mv $file $DATA_DISPATCHED_DIR" $LOG_FILE
					mv $file $DATA_DISPATCHED_DIR
					if [ $server_count -gt 0 ]; then
						SCP_FILE_TOTAL_COUNT=$((SCP_FILE_TOTAL_COUNT+1))
					fi
				#else
				#	mlog "INFO: mv $file $DATA_FAIL_DIR" $LOG_FILE
				#	mv $file $DATA_FAIL_DIR
				#	is_dispatch_succ=0
				fi
			done
		fi
	done

	if [ $is_dispatch_succ -eq 1 -a $SCP_FILE_SUCC_COUNT -gt 0 ]; then
		touch $flag_file
	fi
	infolog "INFO: finish scp file to chunk server, succ_count=$SCP_FILE_SUCC_COUNT, failed_count=$SCP_FILE_FAIL_COUNT"
}

#clear up import directory in each chunkserver
cleanup_cs_import_dir()
{
	# range file line format:
	#   1999^A-1 1001-000000 00000000000007CFFFFFFFFFFFFFFFFF 10.232.36.39 10.232.36.38
	#     1999^A-1  rowkey column separate by special delimeter, ex: \1
	#     1001-000001 table id and tablet range number
	#     00000000000007CFFFFFFFFFFFFFFFFF binary rowkey for hex format
	#     10.232.36.39  the first chunkserver ip
	#     10.232.36.38  the second chunkserver ip
	#     ...           the third chunkserver ip if it exist
	declare -i disp_file_index=0
	declare old_ifs=$IFS
	export IFS=' '
	while read -a array; do
		if [ "X${array[3]}" != "X" ]
		then
			CS_SERVER1[$disp_file_index]=${array[3]}
		fi

		if [ "X${array[4]}" != "X" ]
		then
			CS_SERVER2[$disp_file_index]=${array[4]}
		fi

		if [ "X${array[5]}" != "X" ]
		then
			CS_SERVER3[$disp_file_index]=${array[5]}
		fi
		disp_file_index=$((disp_file_index+1))
	done < $RANGE_FILE
	export IFS=$old_ifs

	declare -a cs_servers=(${CS_SERVER1[@]} ${CS_SERVER2[@]} ${CS_SERVER3[@]})
	declare -a uniq_cs_servers=($(echo ${cs_servers[@]} | sed -e 's/ /\n/g' | sort | uniq))
	declare rm_files="${DST_DATA_DIR}*/${APP_NAME}/import/*"
	for cs in ${uniq_cs_servers[@]}
	do
		declare rm_cmd="ssh ${USER_NAME}@$cs 'rm -rf $rm_files'"
		mlog "INFO: $rm_cmd"
		echo "INFO: $rm_cmd"
		ssh ${USER_NAME}@$cs "rm -rf $rm_files"
	done
}

#insert one fake line into update server and major freeze
#major_free <write_flag> <wait_merge>
major_freeze()
{
	declare -i write_flag=$1
	declare -i wait_merge=$2
	if [ "X$MAJOR_FREEZE" == "X" ]; then
		return 0
	fi

	mlog "INFO: start ups major freeze" $LOG_FILE
	if [ ! -f $SUCCESS_FILE -a $write_flag -gt 0 ]; then
		mlog "INFO: $SUCCESS_FILE isn't existent, can't do major freeze" $LOG_FILE
		echo "INFO: $SUCCESS_FILE isn't existent, can't do major freeze"
		return 1
	fi

	if [ -e $FINISH_UPS_FREEZE -a $write_flag -gt 0 ]; then
		mlog "INFO: $FINISH_UPS_FREEZE is existent, needn't major freeze again" $LOG_FILE
		echo "INFO: $FINISH_UPS_FREEZE is existent, needn't major freeze again"
		return 1
	fi

	if [ "X$ROOTSERVER_IP" == "X" -o "X$UPDATESERVER_IP" == "X" ]; then
		mlog "INFO: not specify rootserver ip ($ROOTSERVER_IP) or updateserver ip ($UPDATESERVER_IP), needn't major freeze" $LOG_FILE
		echo "INFO: not specify rootserver ip ($ROOTSERVER_IP) or updateserver ip ($UPDATESERVER_IP), needn't major freeze"
		return 1
	fi

	# use ob_import tool to insert one line into ups and major freeze 
	declare ctl_data_file="$LOCAL_TABLE_DIR/ctl_data.txt"
	echo -e "0\t0" > $ctl_data_file
	if [ $? -ne 0 -o ! -f $ctl_data_file ]; then
		errorlog "ERROR: create ctl data file ($ctl_data_file) failed"
		return 2
	fi

	if [ -f ${APP_ETC_DIR}import.ini ]; then
		cp ${APP_ETC_DIR}import.ini ${TABLE_ETC_DIR}import.ini
	fi

	declare ob_import_file="${BIN_DIR}ob_import"
	declare import_conf="${TABLE_ETC_DIR}import.ini"
	if [ ! -f $import_conf ]; then
		errorlog "ERROR: $import_conf isn't existent"
		return 2
	fi
	if [ ! -f $ob_import_file ]; then
		errorlog "ERROR: $ob_import_file isn't existent"
		return 2
	fi
	$ob_import_file -h $ROOTSERVER_IP -p $ROOTSERVER_PORT -c $import_conf -t $CTL_TABLE_NAME -f $ctl_data_file
	if [ "X$?" != "X0" ]; then
		errorlog "ERROR: failed to insert ctl data to updateserver by ob_import"
		return 2
	fi

	declare ups_admin_file="${BIN_DIR}ups_admin"
	sleep 10
	$ups_admin_file -a $UPDATESERVER_IP -p $UPDATESERVER_PORT -t major_freeze
	if [ "X$?" != "X0" ]; then
		errorlog "ERROR: failed to major freeze" 
		return 2
	fi

	if [ $write_flag -gt 0 ]; then
		cur_second=$(date "+%s")
		frozen_time_file=$APP_FREEZE_TIME_FILE_PREFIX$cur_second
		rm -rf $APP_FREEZE_TIME_FILE_PREFIX*
		touch $frozen_time_file
	fi

	#use rs_admin tool to enable balance if disable it before
#	sleep 10
#	declare rs_admin_file="${BIN_DIR}rs_admin"
#	if [ ! -f $rs_admin_file ]; then
#		errorlog "ERROR: $rs_admin_file isn't existent"
#		return 2
#	fi
#	$rs_admin_file -r $ROOTSERVER_IP -p $ROOTSERVER_PORT enable_balance
#	if [ "X$?" != "X0" ]; then
#		errorlog "ERROR: failed to send enable balance command to rootserver"
#		return 2
#	fi
	
	touch $FINISH_UPS_FREEZE
	infolog "INFO: finish major freeze"

	if [ $wait_merge -gt 0 ]; then
		infolog "INFO: wait merge done ..."

		declare -i merge_done=0
		declare -i minutes=$((MAJOR_FREEZE_INTERVAL_SECOND/60))
		for ((i=1;i<=$minutes;i++))
		do
			sleep 60
			infolog "INFO: wait merge done $i minutes ..."
			declare rs_admin_file="${BIN_DIR}rs_admin"
			if [ ! -f $rs_admin_file ]; then
				errorlog "ERROR: $rs_admin_file isn't existent"
				return 2
			fi
			declare merge_stat=$($rs_admin_file -r $ROOTSERVER_IP -p $ROOTSERVER_PORT stat -o merge | grep "DONE")
			if [ "X$merge_stat" != "X" ]; then
				merge_done=1
				infolog "INFO: $merge_stat"
				break
			fi
		done

		if [ $merge_done -ne 1 ]; then
			 errorlog "ERROR: after wait $minutes minutes, merge not done"
		else
			if [ $write_flag -gt 0 ]; then
				declare -i cur_second=$(date "+%s")
				declare -i frozen_time=$((cur_second - MAJOR_FREEZE_INTERVAL_SECOND - 120))
				frozen_time_file=$APP_FREEZE_TIME_FILE_PREFIX$frozen_time
				rm -rf $APP_FREEZE_TIME_FILE_PREFIX*
				touch $frozen_time_file
				cleanup_cs_import_dir
			fi
		fi
	fi
}

# remove old sstable data if necessary
remove_old_data()
{
	declare -i last_rm_day=$((RM_DAYS_AGO + 7))
	for ((i=$RM_DAYS_AGO;i<$last_rm_day;i++))
	do
		declare rmdate=$(date -d "$i day ago" +"%Y-%m-%d")
		declare rmdir=$DATA_LOCAL_DIR$APP_NAME/$rmdate
		mlog "INFO: remove old data, old_data_dir=$rmdir" $LOG_FILE
		if [ -d $rmdir ]; then
			rm -rf $rmdir
		fi

		if [ -d $DATA_LOCAL_DIR$APP_NAME ]; then
			declare files=$(ls $DATA_LOCAL_DIR$APP_NAME)
			if [ "X$files" = "X" ]; then
				rm -rf $DATA_LOCAL_DIR$APP_NAME
			fi
		fi

		if [ $GATEWAY_INDEX -eq 0 ]; then
			${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -test -d $HADOOP_DATA_DIR_TOP$APP_NAME/$rmdate
			if [ $? -eq 0 ]; then
				${HADOOP_BIN_DIR}hadoop $HADOOP_BIN_CONFIG_PARAM fs -rmr $HADOOP_DATA_DIR_TOP$APP_NAME/$rmdate 
			fi
		fi
	done
}

# remove lockfile  
remove_lockfile()
{
	mlog "INFO: remove lock file, lockfile=$LOCK_FILE" $LOG_FILE
	if [ $RM_DAYS_AGO -gt $DATA_DAYS_AGO ]; then
		if [ ! -f $LOCK_FILE ]; then
			errorlog "ERROR: $LOCK_FILE loss, perhap reentry!!"
			exit 2
		fi
		rm -f $LOCK_FILE
	fi
	mlog "INFO: release lockfile=$LOCK_FILE." $LOG_FILE
}

# check result
check_result()
{
	declare -i gateway_index=$GATEWAY_INDEX
	if [ "X$DEAD_GATEWAY_INDEX" != "X" ]; then
		gateway_index=$DEAD_GATEWAY_INDEX
	fi
	declare -i total_file_count=${#DISP_FILES[@]}
	declare -i step_size=$((total_file_count/GATEWAY_COUNT))
	declare -i copy_count=$step_size
	if [ $gateway_index -eq $((GATEWAY_COUNT-1)) ]; then
		copy_count=$((total_file_count%GATEWAY_COUNT+step_size))
	fi

	if [ "X$DEAD_GATEWAY_INDEX" != "X" ]; then
		total_file_count=$copy_count
		step_size=$((total_file_count/(GATEWAY_COUNT-1)))
		copy_count=$step_size
		if [ $GATEWAY_INDEX -eq $((GATEWAY_COUNT-1)) ]; then
			copy_count=$((total_file_count%(GATEWAY_COUNT-1)+step_size))
		elif [ $DEAD_GATEWAY_INDEX -eq $((GATEWAY_COUNT-1)) -a $GATEWAY_INDEX -eq $((GATEWAY_COUNT-2))]; then
			copy_count=$((total_file_count%(GATEWAY_COUNT-1)+step_size))
		fi
	fi
	HADOOP_GET_FILE_TOTAL_COUNT=$copy_count
	
	if [ "X$ONLY_COPY_SERVER" != "X" ]; then
		HADOOP_GET_FILE_TOTAL_COUNT=0
		for ((i=0;i<${#DISP_FILES[@]};i++))
		do
			if [ "X${CS_SERVER1[$num]}" != "X" -a "X${CS_SERVER1[$num]}" == "X$ONLY_COPY_SERVER" ]; then
				HADOOP_GET_FILE_TOTAL_COUNT=$((HADOOP_GET_FILE_TOTAL_COUNT+1))
			elif [ "X${CS_SERVER2[$num]}" != "X" -a "X${CS_SERVER2[$num]}" == "X$ONLY_COPY_SERVER" ]; then
				HADOOP_GET_FILE_TOTAL_COUNT=$((HADOOP_GET_FILE_TOTAL_COUNT+1))
			elif [ "X${CS_SERVER3[$num]}" != "X" -a "X${CS_SERVER3[$num]}" == "X$ONLY_COPY_SERVER" ]; then
				HADOOP_GET_FILE_TOTAL_COUNT=$((HADOOP_GET_FILE_TOTAL_COUNT+1))
			fi
		done
	fi

	if [ $HADOOP_GET_FILE_TOTAL_COUNT -ne $SCP_FILE_TOTAL_COUNT ]; then
		errorlog "ERROR: hadoop_get_file_total_count=$HADOOP_GET_FILE_TOTAL_COUNT, scp_file_total_count=$SCP_FILE_TOTAL_COUNT, scp_file_succ_count=$SCP_FILE_SUCC_COUNT, scp_file_fail_count=$SCP_FILE_FAIL_COUNT" > $FAILURE_FILE
		sleep 2
		exit 2
	fi

	infolog "INFO: hadoop_get_file_total_count=$HADOOP_GET_FILE_TOTAL_COUNT, scp_file_total_count=$SCP_FILE_TOTAL_COUNT, scp_file_succ_count=$SCP_FILE_SUCC_COUNT, scp_file_fail_count=$SCP_FILE_FAIL_COUNT"
	if [ $RM_DAYS_AGO -gt $DATA_DAYS_AGO -a $SCP_FILE_TOTAL_COUNT -gt 0 ]; then
		touch $SUCCESS_FILE
	fi
	sleep 2
	exit 0
}

parse_param()
{
	check_param
	parse_config_file_param
	parse_gateway_host_param
}

# init <table_config_file> <sequence_num>
init()
{
	read_param_from_config_file $1
	parse_cli_param $2
	check_dir_format
	global_var_init
	make_data_dir
	check_config_param
	build_range_file
}

# do local dipatch
do_local_dispatch()
{
	build_dispatch_map
	check_reentry
	remove_old_data
	prepare_scp_data
	scp_data_to_cs
	remove_lockfile
	check_result
}

# schedule_dispatch <table_config_file>
schedule_dispatch()
{
	if [ "X$MULT_DISPATCH" != "X" ]; then
		# wait map reduce generate sstable finish
		declare succ_file=$SUCC_MR_GEN_SSTABLE_FLAG_FILE
		declare fail_file=$FAIL_MR_GEN_SSTABLE_FLAG_FILE
		while ((1))
		do
			if [ -f $succ_file ]; then
				infolog "INFO: map reduce generate sstable in hadoop success, APP_NAME=$APP_NAME, TABLE_NAME=$TABLE_NAME"
				break;
			elif [ -f $fail_file ]; then
				errorlog "ERROR: map reduce generate sstable in hadoop failed, APP_NAME=$APP_NAME, TABLE_NAME=$TABLE_NAME, can't copy data from haddop"
				return 2
			else
				sleep 2
			fi
		done

		# remote call the gateway to dispatch sstable files
		declare -i j=0
		declare -i sleep_second=0
		declare -i fail_gateway_index=-1
		for hostname in ${GW_HOSTS[@]}
		do
			infolog "INFO: start gateway [$hostname] to dispatch sstable"
			sleep_second=$((5*j))
			ssh $USER_NAME@$hostname "sleep $sleep_second && ${SCRIPTS_DIR}dispatch.sh -c $1 -i $j -n $GATEWAY_COUNT" &
			j=$((j+1))
		done

		# wait all the gateway servers finish dispatching, and check the return value
		for ((i=0;i<${#GW_HOSTS[@]};i++))
		do
			j=$((i+1))
			wait %$j
			if [ "X$?" != "X0" ]; then
				errorlog "ERROR: gateway [${GW_HOSTS[$i]}] failed to dispatch sstable file"
				GATEWAY_FAIL_COUNT=$((GATEWAY_FAIL_COUNT+1))
				fail_gateway_index=$i
			else
				GATEWAY_SUCC_COUNT=$((GATEWAY_SUCC_COUNT+1))
			fi
		done

		#if one gateway failed, try 5 times
		if [ $GATEWAY_FAIL_COUNT -eq 1 -a $GATEWAY_SUCC_COUNT -gt 0 ]; then
			infolog "INFO: gateway [${GW_HOSTS[$fail_gateway_index]}] failed, retry dispatch"
			for ((i=1;i<=5;i++))
			do
				ssh $USER_NAME@${GW_HOSTS[$fail_gateway_index]} "sleep 5 && ${SCRIPTS_DIR}dispatch.sh -c $1 -i $fail_gateway_index -n $GATEWAY_COUNT" &
				wait %1
				if [ "X$?" != "X0" ]; then
					errorlog "ERROR: gateway [${GW_HOSTS[$fail_gateway_index]}] failed to retry $i time to dispatch sstable file, sleep 1 minute then try again"
					sleep 60
				else
					infolog "INFO: gateway [${GW_HOSTS[$fail_gateway_index]}] retry succ"
					GATEWAY_FAIL_COUNT=0
					break
				fi
			done 

			#after retry, it also fails, deploy the task to another gateways
			if [ $GATEWAY_FAIL_COUNT -eq 1 -a $GATEWAY_SUCC_COUNT -gt 0 ]; then
				j=0
				declare -a alive_gw_hosts=()
				declare -i alive_gw_count=0
				for hostname in ${GW_HOSTS[@]}
				do
					if [ $j -ne $fail_gateway_index ]; then
						infolog "INFO: start gateway [$hostname] to dispatch sstable owned by gateway [${GW_HOSTS[$fail_gateway_index]}]"
						alive_gw_hosts[$alive_gw_count]=$hostname
						alive_gw_count=$((alive_gw_count+1))
						sleep_second=$((5*j))
						ssh $USER_NAME@$hostname "sleep $sleep_second && ${SCRIPTS_DIR}dispatch.sh -c $1 -i $j -n $GATEWAY_COUNT -d $fail_gateway_index -s ${GW_HOSTS[$fail_gateway_index]}" &
					fi
					j=$((j+1))
				done

				declare -i is_retry_succ=1
				for ((i=0;i<$alive_gw_count;i++))
				do
					j=$((i+1))
					wait %$j
					if [ "X$?" != "X0" ]; then
						errorlog "ERROR: gateway [${alive_gw_hosts[$i]}] failed to dispatch sstable file owned by gateway [${GW_HOSTS[$fail_gateway_index]}]"
						is_retry_succ=0
					fi
				done

				if [ $is_retry_succ -ne 1 ]; then
					infolog "INFO: after gateway [${GW_HOSTS[$fail_gateway_index]}] dispatch failed and retry failed, redispatch by another gateways successfully"
					GATEWAY_FAIL_COUNT=0
				else
					errorlog "ERROR: after gateway [${GW_HOSTS[$fail_gateway_index]}] dispatch failed and retry failed, redispatch by another gateways failed"
				fi
			fi
		fi

		if [ $GATEWAY_FAIL_COUNT -gt 0 ]; then 
			errorlog "ERROR: table_config=$1, gateway_succ_count=$GATEWAY_SUCC_COUNT, gateway_fail_count=$GATEWAY_FAIL_COUNT"
			exit 2
		else 
			infolog "INFO: table_config=$1, gateway_succ_count=$GATEWAY_SUCC_COUNT, gateway_fail_count=$GATEWAY_FAIL_COUNT"
		fi
	else
		do_local_dispatch
	fi

	exit 0
}

# map reduce generate sstable for multiple tables and applications,
# we just allow one process to generate sstable in hadoop for tables
# one by one, so we create a lock to do sync
mr_tables()
{
	declare lock_file=${DATA_LOCAL_DIR}mr_gen_sstable.lock
	if [ -f $lock_file ];then
		errorlog "ERROR: map reduce generate sstable re-entry, $lock_file exist!!"
		exit 2
	fi
	touch $lock_file

	declare -i i=0
	declare -a fail_file=()
	declare -i mr_gen_fail=0
	for file in ${CONFIG_FILES[@]}
	do
		init $file $i
		fail_file[$i]=$FAIL_MR_GEN_SSTABLE_FLAG_FILE
		mr_generate_sstable &
		i=$((i+1))
	done

	declare -i j=0
	for ((i=0;i<${#CONFIG_FILES[@]};i++))
	do
		j=$((i+1))
		wait %$j
		if [ "X$?" != "X0" ]; then
			mr_gen_fail=$((mr_gen_fail+1))
			touch ${fail_file[$i]} 
			errorlog "ERROR: failed map-reduce generate sstable, fail_file=${fail_file[$i]}"
		fi
	done

	if [ -f $lock_file ]; then
		rm -rf $lock_file
	fi

	if [ $mr_gen_fail -gt 0 ]; then
		errorlog "ERROR: failed map-reduce generate sstable, fail_count=$mr_gen_fail"
		exit 2
	fi
}

check_root_table_unchanged()
{

	declare -i i=0
	for file in ${CONFIG_FILES[@]}
	do
		init $file $i
		declare cs_admin_file="${BIN_DIR}cs_admin"
		if [ ! -f $cs_admin_file ]; then
			errorlog "ERROR: $cs_admin_file isn't existent"
			return 2
		fi

		if [ ! -f $RANGE_FILE ]; then
			errorlog "ERROR: after dispatch sstable file, $RANGE_FILE isn't existent"
			return 2
		fi

		if [ "X$ROOTSERVER_IP" == "X" ]; then
			mlog "INFO: not specify rooserver ip, needn't scan root table to build range file: $RANGE_FILE" $LOG_FILE
			echo "INFO: not specify rooserver ip, needn't scan root table to build range file: $RANGE_FILE"
			continue
		fi

		mv $RANGE_FILE $RANGE_FILE_OLD
		$cs_admin_file -s $ROOTSERVER_IP -p $ROOTSERVER_PORT -q -i "scan_root_table $TABLE_NAME $TABLE_ID $ROWKEY_SPLIT $APP_NAME $LOCAL_TABLE_DIR $ROWKEY_DELIM_ASCII"
		if [ "X$?" != "X0" ]; then
			errorlog "ERROR: failed to build rowkey range file $RANGE_FILE"
			return 2
		fi

		if [ ! -f $RANGE_FILE ]; then
			errorlog "ERROR: after rebuild range file, $RANGE_FILE isn't existent"
			return 2
		fi

		declare range_file_diff=$(diff $RANGE_FILE $RANGE_FILE_OLD)
		if [ "X$range_file_diff" != "X" ]; then
			errorlog "ERROR: root table is changed, can't do major freeze, before_range_file=$RANGE_FILE_OLD, cur_range_file=$RANGE_FILE"
			return 2
		fi		
		i=$((i+1))
	done
}

# dispatch sstable files for multiple tables and applications
dispatch_tables()
{
	declare -i fail_count=0
	declare -i i=0
	for file in ${CONFIG_FILES[@]}
	do
		init $file $i
		schedule_dispatch $file &
		i=$((i+1))
	done

	declare -i j=0
	for ((i=0;i<${#CONFIG_FILES[@]};i++))
	do
		j=$((i+1))
		wait %$j
		if [ $? -ne 0 ]; then
			fail_count=$((fail_count+1))
			errorlog "ERROR: failed dispatch table data, table_config=${CONFIG_FILES[$i]}"
		fi
	done

	if [ $GATEWAY_INDEX -eq 0 -a $fail_count -eq 0 ]; then
		check_root_table_unchanged
		if [ $? -eq 0 ]; then
			major_freeze 1 1
		fi
	fi

	return $fail_count
}

#check the previous majore freeze time
check_prev_freeze_time()
{
	declare freeze_files=($(ls -t $APP_FREEZE_TIME_FILE_PREFIX*))
	if [ ${#freeze_files[@]} -gt 0 ]; then
		declare prev_freeze_second=${freeze_files[0]#$APP_FREEZE_TIME_FILE_PREFIX}
		declare cur_second=$(date "+%s")
		declare -i diff_second=$((cur_second - prev_freeze_second))
		if [ $diff_second -ge $MAJOR_FREEZE_INTERVAL_SECOND ]; then
			return 0
		else
			errorlog "ERROR: can't do dispatch continously in such short interval, please wait $MAJOR_FREEZE_INTERVAL_SECOND seconds, cur_time=$cur_second, prev_freeze_time=$prev_freeze_second, MAJOR_FREEZE_INTERVAL_SECOND=$MAJOR_FREEZE_INTERVAL_SECOND"
			return 1
		fi
	fi
	return 0
}

# main process function
main_work()
{
	declare -i ret=0
	init ${CONFIG_FILES[0]} 0
	if [ "X$MR_GEN_SSTABLE" != "X" ]; then
		if [ "X$ROOTSERVER_IP" != "X" -a $GATEWAY_INDEX -eq 0 ]; then
			check_prev_freeze_time
			ret=$?
		fi
		if [ $ret -eq 0 ]; then
			if [ "X$MULT_DISPATCH" != "X" ]; then
				declare config_files=$(echo ${ORG_CONFIG_FILES[@]})
				(${SCRIPTS_DIR}dispatch.sh -b -c "$config_files" &)
				dispatch_tables
			else
				mr_tables
			fi
		else
			exit 2
		fi
	elif [ "X$MAJOR_FREEZE" != "X" ]; then
		major_freeze 0 1
	else
		dispatch_tables
	fi
}

########## main ###########
if [ $# -lt 3 ]; then
	usage
	exit 1
fi

while getopts "l:c:g:i:n:d:s:o:hmbf" option
do
	case $option in
	l)
		CONFIG_LIST_FILE=$OPTARG
		;;
	c)
		TABLE_CONFIG_FILES=$OPTARG
		;;
	g)
		GATEWAY_HOST_FILE=$OPTARG
		;;
	i)
		GATEWAY_INDEX=$OPTARG
		;;
	d)
		DEAD_GATEWAY_INDEX=$OPTARG
		;;
	s)
		DEAD_GATEWAY_SERVER=$OPTARG
		;;
	n)
		GATEWAY_COUNT=$OPTARG
		;;
	m)
		MULT_DISPATCH=1
		;;
	b)
		MR_GEN_SSTABLE=1
		;;
	f)
		MAJOR_FREEZE=1
		;;
	o)
		ONLY_COPY_SERVER=$OPTARG
		;;
	h)
		usage
		exit 0
		;;
	\?)
		echo "invalid param"
		usage
		exit 2
		;;
	esac
done
GATEWAY_HOSTS=${!OPTIND}

parse_param
main_work
