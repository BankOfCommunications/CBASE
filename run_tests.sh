#!/bin/bash
# 
# AUTHOR: Zhifeng YANG
# DATE: 2011-08-16
# DESCRIPTION: run all tests in the `tests/' directory
# 

function usage()
{
	echo "run_tests.sh [--print-failed] [test_name_pattern]"
	echo "Example: run_tests.sh "
	echo "         run_tests.sh --print-failed"
	echo "         run_tests.sh base_main_test"
	echo "         run_tests.sh --print-failed rootserver"
	exit 1
}

# parse command line
OPT_PRINT_FAILED=0
TEST_NAME_PATTERN=""
if [ $# -eq 0 ]
then
	true
elif [ $# -eq 1 ]
then
	if [ $1 = "--print-failed" ]
	then
		OPT_PRINT_FAILED=1
	elif [ $1 = "--help" -o $1 = "-h" ]
	then
		usage
	else
		TEST_NAME_PATTERN=$1
	fi
elif [ $# -eq 2 ]
then
	if [ $1 = "--print-failed" ]
	then
		OPT_PRINT_FAILED=1
		TEST_NAME_PATTERN=$2
	else
		usage
	fi
else
	usage
fi

TESTS=`find tests/ -type f -perm /100 \( -name "test_*" -o -name "*_test" \)`
TOPDIR=`pwd`
LOGFILE=$TOPDIR/run_tests.log

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
	if [ $OPT_PRINT_FAILED ]
	then
		./$F 2>&1 |tee -a $LOGFILE|egrep "FAILED"
	else
		./$F 2>&1 |tee -a $LOGFILE|egrep "(\[==========|\[----------|PASSED|FAILED)"
	fi
	cd $TOPDIR
done
