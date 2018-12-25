if [ $# != 1 ];then
	echo "./less.sh ob1.cs0/ups0/rs0/ms0"
	exit 2
fi

cluster=`echo $1 | awk -F'.' '{print $1}'`
cluster_num=${cluster##ob}
if [ x"$cluster_num" = x ];then
     echo "please check input,the cluster num is wrong!"
     exit 2;
fi

role=`echo $1 | awk -F'.' '{print $2}'`

rolename=""
rolenum=-1

if 
echo "$role" | grep -q "ms"
then
	log="mergeserver.log"
	rolename="ms"
	rolenum=`echo ${role:2:1}`

elif 
echo "$role" | grep -q "ups"
then
	log="updateserver.log"
	rolename="ups"
	rolenum=`echo ${role:3:1}`
elif 
echo "$role" | grep -q "rs"
then
	log="rootserver.log"
	rolename="rs"
	rolenum=`echo ${role:2:1}`
elif 
echo "$role" | grep -q "cs"
then
	log="chunkserver.log"
	rolename="cs"
	rolenum=`echo ${role:2:1}`
fi

if [ x"$rolename" = x -o x"$rolenum" = x ];then
	echo "please check input,the role is wrong!"
	exit 2;
fi


if [ -f conf/configure.ini ];then
	servers=`cat conf/configure.ini | grep "$cluster_num.$rolename" | awk -F"=" '{print $2}'`
	if [ x"$servers" = x ];then
		echo "please check input,no server in the configure.ini!"
	fi  
	rolenum=$[ $rolenum + 1 ] 
	ip=`echo $servers | awk -F',' '{print $'$rolenum'}'`
	if [ x"$ip" = x ];then
		echo "please check input,the role num is wrong!"
		exit 2;
	fi
	
	usr=`echo "$USER"`	
	path="/home/$usr/$cluster/log/$log"

	ssh -t $usr@$ip "less $path"
else
	echo "ERROR:conf/configure.ini is not exsit.please check....."
	exit 2;
fi
	

