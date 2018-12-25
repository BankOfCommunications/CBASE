[prepare]

table_num=1

table_names=simple_table

# table fin_influx_payer
simple_table = id(1,1);number(1,1)

[run]

point_query                 = select id, number from simple_table where id=?;1,1,int,1
range_query                 = select id, number from simple_table where id=?;1,1,int,1
range_sum_query             = select id, number from simple_table where id=?;1,1,int,1
range_orderby_query         = select id, number from simple_table where id=?;1,1,int,1
range_distinct_query        = select id, number from simple_table where id=?;1,1,int,1
point_join_query            = select id, number from simple_table where id=?;1,1,int,1
range_join_query            = select id, number from simple_table where id=?;1,1,int,1
range_join_sum_query        = select id, number from simple_table where id=?;1,1,int,1
range_join_orderby_query    = select id, number from simple_table where id=?;1,1,int,1
range_join_distinct_query   = select id, number from simple_table where id=?;1,1,int,1
delete                      = select id, number from simple_table where id=?;1,1,int,1
insert                      = select id, number from simple_table where id=?;1,1,int,1
replace                     = select id, number from simple_table where id=?;1,1,int,1
update_key                  = select id, number from simple_table where id=?;1,1,int,1
update_nonkey               = select id, number from simple_table where id=?;1,1,int,1

#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 
#/*+ read_static */ 


[cleanup]

drop = drop table simple_table;

