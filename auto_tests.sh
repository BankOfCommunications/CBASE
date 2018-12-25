#!/bin/bash
# 

function usage()
{
	echo "auto_tests.sh directory_name"
	exit 1
}

# parse command line
TEST_NAME_PATTERN=""
if [ $# -eq 1 ]
then
	true
else
	usage
fi

TESTS=`find $1 -type f -perm /100 \( -name "test_*" -o -name "*_test" \)`
TOPDIR=`pwd`

for T in $TESTS
do
	if [ -n $TEST_NAME_PATTERN ]
	then
		case $T in
			*${TEST_NAME_PATTERN}*) ;;
			*) continue ;;
		esac
	fi
	D=`dirname $T`
	F=`basename $T`
	echo "[****************] RUN TEST $F IN $D"
	cd $D
	./$F 2>&1
	cd $TOPDIR
done
