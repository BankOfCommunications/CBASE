#!/bin/bash

#============ Input =============

#------------ Basic -------------

    # The following 5 command executes at the 1st time.
    #If you haven't had deploy directory, you need svn check.
#FLAG_SVN_CHECKOUT_DEPLOY=1
   #To copy bin & lib to deploy dir
#FLAG_COPY=1
   #To modify config.py & deploy.py
#FLAG_CONFIG=1
   #To reboot the deploy cluster
#FLAG_REBOOT=1
   #To test a simple case to make sure deploy tool normal
#FLAG_SIMPLE_TEST=1

    #name of test cases split by ',' like ie_all_types,ie_chinese,ie_trim [add dinggh count=22,ignore_merge]
TEST_CASE=ie_add_quotes,ie_all_types,ie_charset,ie_chinese_word,ie_combine_columns,ie_conf_column_format,ie_conf_delpoint,ie_diff_delimas,ie_double_delima,ie_ins_rep_del,ie_keep_double,ie_quote,ie_special_delima,ie_specified_columns,ie_table_name,ie_tables_join,ie_trim,ie_varchar_not_null,ie_column_mapping,ie_incorrect_data,ie_incorrect_column_mapping,ie_charset_fail,ie_int32,ie_int,ie_decimal
#TEST_CASE=ie_table_name
#TEST_CASE=ie_int32,ie_int,ie_decimal
#   #To execute in record mode, namely to execute t/xxx.test to make record/xxx.record which can be changed into r/xxx.result for correctness checking after.
#FLAG_RECORD=1
#   #To move xxx.record to xxx.result
#FLAG_RECORD_2_RESULT=1
#   #To test a case like t/xxx.test, if r/xxx.result has existed.
FLAG_TEST=1

#---------- Advanced -----------

        DIR=$HOME/deploy
    SRC_BIN=$HOME/oceanbase/bin
    SRC_LIB=$HOME/oceanbase/lib
     DST_CB=$HOME/cb4deploy
DST_CB_DATA=$DST_CB/data

 RS=182.119.80.60
UPS=$RS
 CS=$RS
 MS=$RS
   PORT_PREFIX=7
       RS_PORT=${PORT_PREFIX}500
      UPS_PORT=${PORT_PREFIX}700
       MS_PORT=${PORT_PREFIX}800
       CS_PORT=${PORT_PREFIX}600
UPS_INNER_PORT=${PORT_PREFIX}701
    MYSQL_PORT=${PORT_PREFIX}880

#================================

TEST_CASES=( `echo "$TEST_CASE" | sed 's/,/ /g'` )

function yes
{
        if [[ "$1" != "" ]] && [[ "$1" != "0" ]] && [[ "$1" != "false" ]]
        then
                return 0
        else
                return 1
        fi
}

if ! ls $SRC_BIN >/dev/null || ! ls $SRC_LIB >/dev/null
then
	exit 1
fi

if [ ! -d $DIR ]
then
	if ! yes $FLAG_SVN_CHECKOUT_DEPLOY
	then
		echo "ERROR: $DIR doesn't exist. You should svn-checkout by FLAG_SVN_CHECKOUT_DEPLOY=1"
		exit 1
	fi
else
	if yes $FLAG_SVN_CHECKOUT_DEPLOY
	then
		echo "WARN: $DIR has existed. svn checkout ignored."
	fi
fi

if yes $FLAG_SVN_CHECKOUT_DEPLOY
then
	svn checkout svn://182.119.174.53/bankcomm_ob_basever/trunk/oceanbase/tools/deploy $DIR
fi

cd $DIR

if yes $FLAG_COPY
then
	echo "========== Copy bin & lib ========="
	cp -r $SRC_BIN $SRC_LIB .
	cp $SRC_BIN/rs_admin $SRC_BIN/ups_admin $SRC_BIN/ob_*port* tools
fi

if yes $FLAG_CONFIG
then

echo "========== Modify config.py ========="
sed -i '
s#^data_dir = '\''[^'\'']*'\''#data_dir = '\'$DST_CB_DATA\''#g;
s#^rs0_ip = '\''[^'\'']*'\''#rs0_ip = '\'$RS\''#g;
s#^ups1_ip = '\''[^'\'']*'\''#ups1_ip = '\'$UPS\''#g;
s#^cs0_ip = '\''[^'\'']*'\''#cs0_ip = '\'$CS\''#g;
s#^ms0_ip = '\''[^'\'']*'\''#ms0_ip = '\'$MS\''#g;
s#^ob1=OBI( rs_port=[^,]*,ups_port=[^,]*,ms_port=[^,]*,cs_port=[^,]*,ups_inner_port=[^,]*,mysql_port=[^,]*,#ob1=OBI( rs_port='$RS_PORT',ups_port='$UPS_PORT',ms_port='$MS_PORT',cs_port='$CS_PORT',ups_inner_port='$UPS_INNER_PORT',mysql_port='$MYSQL_PORT',#g;
' config.py

echo "========== Modify deploy.py ========="
sed -i '
s#^ \&\& rsync -a .*/bin# \&\& rsync -a \$workdir'$DST_CB'/bin#g;
s#^    dir = '\''[^'\'']*'\''#    dir = '\'$DST_CB\''#g;
' deploy.py

echo "========== chmod ========="
chmod 755 mysqltest deploy.py tools/rs_admin tools/ups_admin
fi

if yes $FLAG_REBOOT
then
echo "========== deploy test ========="
./deploy.py ob1.reboot
fi

if yes $FLAG_SIMPLE_TEST
then
echo "========== test ========="
./deploy.py ob1.mysqltest testset=replace test disable-reboot
fi

if yes $FLAG_RECORD || yes $FLAG_RECORD_2_RESULT || yes $FLAG_TEST
then
	for test_case in ${TEST_CASES[@]}
	do
		if ! ls mysql_test/t/$test_case.test >/dev/null
		then
			exit 1
		fi
	done
fi

if yes $FLAG_RECORD
then
echo "========== deploy.py ========="
./deploy.py ob1.mysqltest testset=$TEST_CASE record disable-reboot
fi

if yes $FLAG_RECORD_2_RESULT
then
	echo "========== record to result ========="
	for test_case in ${TEST_CASES[@]}
	do
		cp mysql_test/record/$test_case.record mysql_test/r/$test_case.result
	done
fi

if yes $FLAG_TEST
then
echo "========== deploy.py ========="
./deploy.py ob1.mysqltest testset=$TEST_CASE test disable-reboot
fi
