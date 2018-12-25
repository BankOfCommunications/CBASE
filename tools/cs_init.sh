#/bin/bash
#usage: ./cs_init app_name root_vip

if [ -d /home/admin/oceanbase/data ]; then
        echo "data dir exist!"
        exit 1
fi

if [ -z $1 ]; then
        echo "input app name!"
        exit 2
fi

disk_num=`df -h | grep "/data/[0-9][0-9]*" | wc -l`

for (( i = 1; i <= ${disk_num}; i++)); do
	#echo ${i}
        mkdir -p /data/${i}/$1/sstable
        mkdir -p /data/${i}/Recycle
done

mkdir /home/admin/oceanbase/data
cd /home/admin/oceanbase/data
for (( i = 1; i <= ${disk_num}; i++)); do
	#echo ${i}
        ln -s /data/${i}/ ${i}
done

sed "s#appapp_name#${1}#g" /home/admin/oceanbase/etc/chunkserver.conf.template  | sed "s#rootroot_vip#${2}#g" | grep -v "\#" > /home/admin/oceanbase/etc/chunkserver.conf
sed "s#rootroot_vip#${2}#g" /home/admin/oceanbase/etc/mergeserver.conf.template | grep -v "\#" > /home/admin/oceanbase/etc/mergeserver.conf

