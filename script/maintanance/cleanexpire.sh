#!/bin/bash

##### HOMEĿ¼ ###############
REAL_FILE=`readlink -f $0`
cd `dirname $REAL_FILE`/..
BASE_HOME="`pwd`"
##############################

if [ -d "$BASE_HOME/logs" ]; then
  cd $BASE_HOME/logs
  find . -ctime +10 -a -type f -a -name 'updateserver.log.*' -exec rm -f {} \;
  find . -ctime +10 -a -type f -a -name 'rootserver.log.*' -exec rm -f {} \;
  find . -ctime +10 -a -type f -a -name 'chunkserver.log.*' -exec rm -f {} \;
  find . -ctime +10 -a -type f -a -name 'mergeserver.log.*' -exec rm -f {} \;
fi
