#!/bin/bash

count=1000
base=0
schema_file=
dest_file=
usage()
{
	echo "./build_raw_data.sh -s [schema file] 
		    -d [dest file] 
		    -c [count default 1000] 
		    -b [base default 0]"
	exit 1;
}

while getopts "s:d:c:b:" options
do
	case $options in
	  s)
	    schema_file=$OPTARG
	    ;;
	  d)
	    dest_file=$OPTARG
	    ;;
	  c)
	    count=$OPTARG
	    ;;
	  b)
	    base=$OPTARG
	    ;;
	  \?)
	    usage
	    ;;
	esac
done

if [ -z $schema_file -o -z $dest_file ]
then
	usage
fi

>$dest_file

fields=`awk -F ',' '{print $4}' $schema_file |sed -e "/^$/d"`

for((i=$base;i<$count+$base;++i))
do
	row=`printf "%08d" $i`0row_key_len_isnot_17
	for field in $fields
	do
		case $field in
			int)
			  row+="|$i";
			;;
			varchar)
			  row+="|varchar$i";
			;;
			create_time|modify_time|datetime|precise_datetime)
			  row+="|`date '+%Y-%m-%d %H:%M:%S'`"
			;;
			float)
			  row+="|1.0"
			;;
			double)
			  row+="|2.0"
			;;
		esac
	done
	echo $row >> $dest_file
done
