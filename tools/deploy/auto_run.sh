source /tmp/udistcc;
source /home/rizhao.ych/.bash_profile;
pwd=${PWD}
update="yes"
cluster="ob3"
url=http://svn.app.taobao.net/repos/oceanbase/branches/rev_0_3_0_ii_dev/oceanbase
while getopts "c:a:n" arg
do
  case $arg in
    a)
      url=$OPTARG
      echo "address:$url"
      ;;
    c)
      cluster=$OPTARG
      echo "cluster:$cluster"
      ;;
    n)
      echo "do not checkout"
      update="no"
      ;;
  esac
done

if [ x$update = x"yes" ]; then
  mkdir -p auto_run;
  cd auto_run; svn co $url
  cd oceanbase;
  ./build.sh init; ./configure --with-release; 
  cd src;
  make -j;
  cd ../;
  cd $pwd; 
  ./copy.sh $pwd/auto_run/oceanbase;
fi

#deploy bigquery

./deploy.py ${cluster}.random_test check 5 >check_err_${cluster};

basic_err=1;
if grep "not pass basic check before timeout" check_err_${cluster} >basic_err_${cluster}; then
  basic_err=0;
fi
result_err=1;
if grep "ERROR" check_err_${cluster} >result_err_${cluster}; then
  result_err=0;
fi

#echo "basic_err_${cluster}="$(eval "echo \$basic_err_${cluster}")#,result_err_${cluster}="$result_err;
echo "basic_err_${cluster}="$basic_err",result_err_${cluster}="$result_err;

test_name="unknown_test"
if [ $cluster = "ob1" ]; then
  test_name="mixed_test"
elif [ $cluster = "ob2" ]; then
  test_name="sys_checker"
elif [ $cluster = "ob3" ]; then
  test_name="bigquery"
elif [ $cluster = "ob4" ]; then
  test_name="sqltest"
fi

if [ $basic_err -eq 1 ] && [ $result_err -eq 1 ]; then
  #send success msg
#/u01/agent/bin/insert_msg_queue.pl -m "svn:${url} ${test_name} daily regression SUCC" -t w
  echo "re-deploy";
  ./deploy.py ${cluster}.ct.stop; ./deploy.py ${cluster}.stop; ./deploy.py ${cluster}.[conf,rsync];./deploy.py ${cluster}.restart;
  sleep 600 #wait ob recover
  ./deploy.py ${cluster}.proxyserver[0,1,2,3,4,5,6].stop; ./deploy.py ${cluster}.start;
  ./deploy.py ${cluster}.ct.reboot;
else
  #send error msg
  err_msg=`grep "ERROR\\\\|not pass basic check before timeout" check_err_${cluster} | head -n 5`
  /u01/agent/bin/insert_msg_queue.pl -m "svn:${url} ${test_name} daily regression FAIL ${err_msg}" -t w
  echo "failed"
fi

##clean
/bin/rm -rf auto_run

