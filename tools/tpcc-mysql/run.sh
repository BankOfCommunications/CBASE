mysql -h10.232.23.14 -P2848  -uadmin -padmin test < create_table.sql    
mysql -h10.232.23.14 -P2848  -uadmin -padmin test < count.sql    
sh load.sh test 1
./tpcc_start -h 10.232.23.14 -P2848 -d test -u admin -p admin -w 1 -c 4 -r 10 -l 60 -i 10 
