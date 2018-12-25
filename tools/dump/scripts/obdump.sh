#!/bin/sh

BIN_DUMP=./bin/obdump
CONF=conf/dumper.ini

case $1 in
	start)
    ${BIN_DUMP} -c ${CONF} -d
		;;
	stop)
    kill -9 `pgrep obdump`
		;;
	*)
		echo "Usage: $0 (start | stop)"
		;;
esac

