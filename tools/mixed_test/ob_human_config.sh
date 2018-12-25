#!/bin/sh

host=10.232.36.176
port=27048
user=admin
passwd=admin
type=$1
conf=$2

if [ "$type" = "rootserver" ]
then
  tn=1
elif [ "$type" = "chunkserver" ]
then
  tn=2
elif [ "$type" = "mergeserver" ]
then
  tn=3
elif [ "$type" = "updateserver" ]
then
  tn=4
else
  echo -e "\033[31mInvalid server type [$type]\033[0m"
  exit -1
fi

n=0
c=0
grep "^#" $conf -v | egrep "=|\[" | while read line
do
  serv="[$type]"
  echo $line | grep "=" -q
  if [ $? -ne 0 ]
  then
    line=`echo $line | sed "s/_//g"`
    if [ "$line" = $serv ]
    then
      c=0
    elif [ "$line" = "[public]" ]
    then
      c=0
    else
      c=1
    fi
    continue
  fi
  if [ $c -eq 1 ]
  then
    continue
  fi
  name=`echo $line | awk -F "=" '{print $1}' | awk '{print $1}'`
  value=`echo $line | awk -F "=" '{print $NF}' | awk '{print $NF}'`
  if [ -n "$value" ]
  then
    oldv=`echo "select value from __all_sys_config_stat where name='$name'" | mysql -h $host -P $port -u $user -p$passwd | tail -n 1`
    if [ "$oldv" = "$value" ]
    then
        echo | awk -v n="$name" -v v="[$value]" -v o="[$oldv]" -v t=$type '{printf "\033[34m[%s][Noop] % -40s from % -20s to % -20s\033[0m\n", t, n, o, v}'
    else 
      sql="replace into __all_sys_config (cluster_role,cluster_id,server_type,server_role,server_ipv4,server_ipv6_high,server_ipv6_low,server_port,name,value) values (0,0,$tn,0,0,0,0,0,'$name','$value');"
      echo $sql | mysql -h $host -P $port -u $user -p$passwd
      if [ $? -eq 0 ]
      then
        echo | awk -v n="$name" -v v="[$value]" -v o="[$oldv]" -v t=$type '{printf "\033[32m[%s][Reload] % -40s from % -20s to % -20s\033[0m\n", t, n, o, v}'
      else
        echo -e "\033[31mRun sql fail [$sql]\033[0m"
      fi
    fi
  else
  echo -e "\033[31mName=$name value is empty\033[0m"
  fi
done

echo -e "To apply the configs, you need \"\033[31mups_admin -a [serv_addr] -p [serv_port] -t reload_conf\033[0m\""

