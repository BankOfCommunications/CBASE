#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 group_name";
    exit 1;
fi

##### HOMEĿ¼ ###############
#REAL_FILE=`readlink -f $0`
#cd `dirname $REAL_FILE`/..
#export BASE_HOME="`pwd`"
##############################
oblist="Ob1"
eval "single=\$$1"
single="`echo $single | sed -e 's/ /\n/g' | awk '{print $1 \" \"  substr($1, 4)}' | sort -k 2 -n | awk '{print $1}'`"
if [ -n "$single" ]
then
echo $single
else
echo $1
fi
