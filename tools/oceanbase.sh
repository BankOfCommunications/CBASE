#!/bin/bash

DIR=`dirname $0`
BIN_DIR=${DIR}/bin
ETC_DIR=${DIR}/etc
LIB_DIR=${DIR}/lib


CS_CMD=chunkserver
CS_CONF=chunkserver.conf

MS_CMD=mergeserver
MS_CONF=mergeserver.conf

UPS_CMD=updateserver
UPS_CONF=updateserver.conf

RS_CMD=rootserver
RS_CONF=rootserver.conf


set_env()
{
	export LD_LIBRARY_PATH=$LIB_DIR:/usr/local/lib:/usr/local/lib64:$LD_LIBRARY_PATH
	ulimit -n 65535 >/dev/null 2>&1
	ulimit -c unlimited
}

#$1 cmd
#$2 conf
start_service()
{
	$1 -f $2
}

#$1 cmd
stop_service()
{
	killall $1
}


#main

set_env
cd $DIR

case $1 in
	start_cs)
		start_service $BIN_DIR/$CS_CMD ${ETC_DIR}/$CS_CONF
		;;
	start_ms)
		start_service $BIN_DIR/$MS_CMD ${ETC_DIR}/$MS_CONF
		;;
	start_ups)
		start_service $BIN_DIR/$UPS_CMD ${ETC_DIR}/$UPS_CONF
		;;
	start_rs)
		start_service $BIN_DIR/$RS_CMD ${ETC_DIR}/$RS_CONF
		;;
	stop_cs)
		stop_service ${CS_CMD}
		;;
	stop_ms)
		stop_service ${MS_CMD}
		;;
	stop_ups)
		stop_service ${UPS_CMD}
		;;
	stop_rs)
		stop_service ${RS_CMD}
		;;
	clean)
		rm  log  -rf
		;;
	*)
		echo "Usage: $0 start_cs|stop_cs|start_ms|stop_ms|start_rs|stop_rs|start_ups|stop_ups|clean)"
		;;
esac

