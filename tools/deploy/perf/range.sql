[prepare]

table_num=1

table_names=range_table

range_table = id(1,1);number(1,1)

[run]

point_query                 = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_query                 = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_sum_query             = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_orderby_query         = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_distinct_query        = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
point_join_query            = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_join_query            = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_join_sum_query        = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_join_orderby_query    = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
range_join_distinct_query   = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
delete                      = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
insert                      = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
replace                     = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
update_key                  = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100
update_nonkey               = select id, number from range_table where id between ? and ?;100,2,int,1,int,2=1+100

[cleanup]

drop = drop table range_table;

