[prepare]

table_num=1

table_names=simple_row_table

# table fin_influx_payer
simple_row_table = id(1,1);number(1,1)

[run]

point_query                 = insert into single_row_table values(?,?);1,2,int,1,int,1
range_query                 = insert into single_row_table values(?,?);1,2,int,1,int,1
range_sum_query             = insert into single_row_table values(?,?);1,2,int,1,int,1
range_orderby_query         = insert into single_row_table values(?,?);1,2,int,1,int,1
range_distinct_query        = insert into single_row_table values(?,?);1,2,int,1,int,1
point_join_query            = insert into single_row_table values(?,?);1,2,int,1,int,1
range_join_query            = insert into single_row_table values(?,?);1,2,int,1,int,1
range_join_sum_query        = insert into single_row_table values(?,?);1,2,int,1,int,1
range_join_orderby_query    = insert into single_row_table values(?,?);1,2,int,1,int,1
range_join_distinct_query   = insert into single_row_table values(?,?);1,2,int,1,int,1
delete                      = insert into single_row_table values(?,?);1,2,int,1,int,1
insert                      = insert into single_row_table values(?,?);1,2,int,1,int,1
replace                     = insert into single_row_table values(?,?);1,2,int,1,int,1
update_key                  = insert into single_row_table values(?,?);1,2,int,1,int,1
update_nonkey               = insert into single_row_table values(?,?);1,2,int,1,int,1

[cleanup]

drop = drop table simple_table;

