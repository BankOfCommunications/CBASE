#!/bin/bash

#$1 schema_file
#$2 raw data file
#$3 dest path
#$4 disk_no

CMD_PATH=`dirname $0`

if [ x$1 = x -o x$2 = x -o x$3 = x -o x$4 = x ]
then 
	echo "Usage ./build_data.sh [schema file] [raw data file] [dest path] [disk no]"
	exit 1;
fi

table_id=`grep "table_id" $1`
table_id=${table_id#*=}

echo " cmd is ./build_data.sh -s $1 -f $2 -d $3 -t $table_id -i $4"

${CMD_PATH}/build_data -s $1 -f $2 -d $3 -t $table_id -i $4



