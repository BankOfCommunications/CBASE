[prepare]

table_num=1

table_names=simple_table

# table fin_influx_payer
simple_table = id(1,1);number(1,1)

[run]

point_query                 = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_query                 = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_sum_query             = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_orderby_query         = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_distinct_query        = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
point_join_query            = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_join_query            = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_join_sum_query        = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_join_orderby_query    = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
range_join_distinct_query   = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
delete                      = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
insert                      = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
replace                     = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
update_key                  = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0
update_nonkey               = update single_row_table set number = number + 1 where id = 1 and number < 100000000;1,0

[cleanup]

drop = drop table simple_table;

