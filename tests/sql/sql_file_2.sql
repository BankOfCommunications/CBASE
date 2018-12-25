--------------------------------------------------------------------------------------------------------------------
-- create table user
-- (
--	user_id	 int,
--	user_name char(20),
--	user_age int,
--	user_desc varchar(500)
-- );

-- create table item
-- (
--	item_id int,
--	item_name varchar(200),
--	item_cost int,
--	item_price int,
--	item_color char
-- );

-- create table order_list
-- (
--	id int,
--	user_id int,
--	item_id int,
--	qty int,
--	order_desc varchar(500),
--	order_time date
-- );
--------------------------------------------------------------------------------------------------------------------

@@ sql_join_1
-- ok
select user_name, id, qty from user left outer join order_list on user.user_id = order_list.user_id;

@@ sql_join_2
-- ok
select id, item_name, qty from order_list o right outer join item as i on o.item_id = i.item_id;

@@ sql_join_3
-- ok
select id, user_name, item_name, qty from user left join order_list o on user.user_id = o.user_id right outer join item as i on o.item_id = i.item_id;

@@ sql_join_4
-- fail, unknown table not_exist_table
select user_name, id, qty from user left outer join not_exist_table on user.user_id = not_exist_table.user_id;

@@ sql_join_5
-- fail, unknown table not_exist_table
select user_name, id, qty from user left outer join not_exist_table on user.user_id = not_exist_table.user_id;

@@ sql_join_6
-- ok
select user_name, id, qty from user, order_list where user.user_id > order_list.user_id;

@@ sql_join_7
-- ok
select u.*, o.*, i.* from user u, order_list o, item i where i.item_id = order_list.item_id and u.user_id > o.user_id;

@@ sql_join_8
-- ok
select u.*, o.*, i.* from user u, order_list o, item i where i.item_id > order_list.item_id and u.user_id > o.user_id;


@@ sql_table_scan_1
-- ok
select * from user where my_udf(user_id) > 0 and user_age > 20 and user_id = 100;

@@ sql_table_scan_2
-- ok
select user_name, id, qty from user, order_list where user.user_id > order_list.user_id and user_age > 20 and item_id between 0 and 100;

@@ sql_table_scan_3
-- ok
select id, qty from order_list where order_desc like '%happy%';


@@ sql_filter_1
-- ok
select id, qty from order_list where length(order_desc) > 5 or user_id < 1000;

@@ sql_filter_2
-- ok
select id, qty from order_list o, item i where o.item_id = i.item_id or item_cost > 10;

@@ sql_filter_3
-- ok
select user_id, sum(qty) from order_list where user_id < 1000 group by user_id having max(qty) > 100;

@@ sql_filter_4
-- fail,
select user_id, sum(qty) as total from order_list where user_id < 1000 and total >100;

@@ sql_filter_5
-- fail, Invalid use of group function
select user_id, sum(qty) as total from order_list where user_id < 1000 and max(qty) >100;

@@ sql_filter_6
-- ok
select user_id, sum(qty) as total from order_list where user_id < 1000 having total >100;


@@ sql_aggregate_1
-- ok
select item_id, user_id, sum(qty) from order_list group by item_id, user_id;

@@ sql_aggregate_2
-- ok
select item_id, user_id, sum(qty) from order_list group by user_id;

@@ sql_aggregate_3
-- ok
select sum(qty), max(qty), min(qty), count(*), count(qty), avg(qty) from order_list;

@@ sql_aggregate_4
-- ok
select sum(distinct qty), max(distinct qty), min(distinct qty), count(*), count(distinct qty), avg(distinct qty) from order_list group by user_id;


@@ sql_distinct_1
-- ok
select distinct * from order_list;

@@ sql_distinct_2
-- ok
select distinct * from order_list order by 1 asc, 2 desc;

@@ sql_distinct_3
-- ok
select distinct id, user_id, qty from order_list order by item_id desc;


@@ sql_sort_1
-- ok
select i.*, o.* from item i, order_list o where i.item_id = o.item_id;

@@ sql_sort_2
-- ok
select i.*, o.* from item i, order_list o where i.item_id = o.item_id + 1;

@@ sql_sort_3
-- ok
select * from order_list group by user_id, item_id;

@@ sql_sort_4
-- ok
select user_id, item_id, sum(distinct qty) from order_list group by user_id + item_id;

@@ sql_sort_5
-- ok
select distinct user_id, item_id from order_list;

@@ sql_sort_6
-- ok
select distinct user_id + item_id, qty from order_list;

@@ sql_sort_7
-- ok
select distinct user_id + item_id, qty from order_list order by user_id, item_id;

@@ sql_sort_8
-- ok
select distinct user_id + item_id, qty from order_list order by 1;

@@ sql_sort_9
-- fail, Unknown column '3' in 'order clause'
select distinct user_id + item_id, qty from order_list order by 3;


@@ sql_limit_1
-- ok
select * from order_list limit 0, 50;

@@ sql_limit_2
-- ok
select * from order_list limit 50, 0;

@@ sql_limit_3
-- ok
select * from order_list offset 50;

@@ sql_limit_4
-- ok
select * from order_list;


@@ sql_project_1
-- ok
select id from order_list where qty > 100 order by user_id;

@@ sql_project_2
-- ok
select id, qty/1000 unit from order_list where qty > 100 order by user_id;

@@ sql_project_3
-- ok
select user_id, item_id, sum(qty) from order_list group by user_id + item_id;

@@ sql_project_4
-- ok
select distinct id, user_id + item_id, qty from order_list;

@@ sql_project_5
-- ok
select distinct id, user_id id, item_id id, qty from order_list;


@@ sql_sub_query_1
-- ok
select id, qty from (select * from order_list) o where user_id > 50;

@@ sql_sub_query_2
-- ok
select * from order_list where qty = (select max(qty) from order_list);

@@ sql_sub_query_3
-- ok
select id, qty, (select count(*) from order_list) total_count from order_list where qty = (select max(qty) from order_list);

@@ sql_sub_query_4
-- ok
select * from order_list where id < 100 union all select * from order_list where id > 100;

@@ sql_sub_query_5
-- ok
select * from order_list where id < 100 union distinct select * from order_list where id > 100;

@@ sql_sub_query_6
-- ok
select * from order_list where user_id < 100 intersect select * from order_list where item_id > 0;

@@ sql_sub_query_7
-- ok
select * from order_list where id < 100 intersect distinct select * from order_list where id > 100;

@@ sql_sub_query_8
-- ok
select * from order_list where id < 100 except select * from order_list where id > 100;

@@ sql_sub_query_9
-- ok
select * from order_list where id < 500 except distinct select * from order_list where id > 100;


@@ sql_alias_1
-- ok
select *, user_id as uid from user where uid < 100;

@@ sql_alias_2
-- ok
select case when user_id < 1000 then user_name else 'young user ' || user_name end as name from user where name like '%young%';

@@ sql_alias_3
-- ok
select * from user u, order_list as o, item;


@@ sql_group_1
-- ok
select user_id as uid, sum(qty) as total from order_list where user_id < 1000 group by uid;

@@ sql_group_2
-- fail, Invalid use of alias which contains group function
select user_id as uid, sum(qty) as total from order_list where user_id < 1000 group by total;

@@ sql_group_3
-- fail, Unknown column '0' in 'group clause'
select user_id, sum(qty) from order_list where user_id < 1000 group by 0;

@@ sql_group_4
-- ok
select user_id, sum(qty) from order_list where user_id < 1000 group by 1;

@@ sql_group_5
-- fail, Invalid use of alias which contains group function
select user_id, sum(qty) from order_list where user_id < 1000 group by 1, 2;


@@ sql_where_1
-- ok
select user_id uid, sum(qty) as total from order_list where uid < 1000;

@@ sql_where_2
-- fail, Invalid use of alias which contains group function
select user_id uid, sum(qty) as total from order_list where uid < 1000 and total >100;


@@ sql_order_1
-- fail, Unknown column '0' in 'order clause'
select user_id uid, sum(qty) as total from order_list where uid < 1000 order by 0;

@@ sql_order_2
-- ok
select user_id uid, sum(qty) as total from order_list where uid < 1000 order by 1;

@@ sql_order_3
-- ok
select user_id uid, sum(qty) as total from order_list where uid < 1000 order by 1, 2;