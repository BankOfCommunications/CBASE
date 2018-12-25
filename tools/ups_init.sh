#!/bin/bash
#usage: ./ups_init app_name root_vip ups_vip ups_0_ip ups1_ip 

if [ -d /home/admin/oceanbase/ups_data ]; then
        echo "data dir exist!"
        exit 1
fi

ups0=${4}
if [ ! ${5} ]; then
   ups1="null"
fi

disk_num=`df -h | grep "/data/[0-9][0-9]*" | wc -l`
raid_num=`echo "${disk_num}/2"|bc`

mkdir /data/log1/root_commitlog
mkdir /data/log1/ups_commitlog

for (( i = 1; i <= ${disk_num}; i++)); do
   mkdir -p /data/${i}/ups_data/trash
done

mkdir /home/admin/oceanbase/ups_data
for (( i = 1; i <= ${raid_num}; i++)); do
   mkdir /home/admin/oceanbase/ups_data/raid${i}
done

cd /home/admin/oceanbase/
ln -s /data/log1/root_commitlog/ root_commitlog
ln -s /data/log1/ups_commitlog/ ups_commitlog

j=1
for (( i = 1; i <= ${raid_num}; i++)); do
   cd /home/admin/oceanbase/ups_data/raid${i}
   ln -s /data/${j}/ups_data/ store1
   let j=j+1
   ln -s /data/${j}/ups_data/ store2
   let j=j+1
done

sed "s#obiobi_0_vip#${2}#g" /home/admin/oceanbase/etc/rootserver.conf.template  | sed "s#rootroot_vip#${2}#g" | sed "s#upsups_vip#${3}#g" | grep -v "\#" > /home/admin/oceanbase/etc/rootserver.conf
sed "s#rootroot_vip#${2}#g" /home/admin/oceanbase/etc/updateserver.conf.template | sed "s#upsups_vip#${3}#g" | sed "s#appapp_name#${1}#g" | sed "s#upsups_0_ip#${ups0}#g" | sed "s#upsups_1_ip#${ups1}#g" | grep -v "\#" > /home/admin/oceanbase/etc/updateserver.conf
