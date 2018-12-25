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

@@ sql_outer_join_1
-- ok
select u.user_id+1000 as standard_uid, user_name, sum(qty) from user as u join order_list o on u.user_id =o.user_id, item as i where o.item_id=i.item_id group by standard_uid having standard_uid > 500;

@@ sql_column_1
-- ok
select id,qty, order_desc from (select id, qty, t.order_desc from order_list t) o;
@@ sql_column_2
-- fail, Unkown column name qty
select user_id, qty, order_desc from (select user_id, sum(qty), t.order_desc, t.order_desc from order_list t group by user_id) o;
@@ sql_column_3
-- fail, column order_desc is ambiguous
select user_id, qty, order_desc from (select user_id, sum(qty) qty, t.order_desc, t.order_desc from order_list t group by user_id) o;
@@ sql_column_4
-- ok
select user_id, qty, descr from (select user_id, sum(qty) qty, t.order_desc descr, t.order_desc from order_list t group by user_id) o;
@@ sql_column_5
-- ok, allow to use alias name
select user_id, sum(qty) sum_qty, sum_qty+1000 std_qty from order_list group by user_id;

-- we can not compare complicated expression now
-- so only single column or alias in select cause can appear as IDENT in having clause

@@ sql_column_alias_1
-- ok allow alias name of same name in table
select user_id user_id from order_list;
@@ sql_column_alias_2
-- ok allow same alias names
select user_id myid, item_id myid from order_list;
@@ sql_column_alias_3
-- ok, ambiguous alias name id is not referenced
select item_id+user_id id, user_id+item_id as id, sum(qty) sum_qty from order_list group by item_id+user_id;

@@ sql_having_1
-- fail, Unknown item_id in having clause
select user_id, sum(qty) sum_qty from order_list group by user_id having item_id >9;
@@ sql_having_2
-- ok, item_id is single column
select user_id, sum(qty) sum_qty from order_list group by item_id having item_id >9;
@@ sql_having_3
-- fail, Unknown item_id/user_id in having clause
select item_id+user_id, sum(qty) sum_qty from order_list group by item_id+user_id having item_id+user_id >9;
@@ sql_having_4
-- ok, using alias name in having clause
select item_id+user_id id, sum(qty) sum_qty from order_list group by item_id+user_id having id >9;
@@ sql_having_5
-- fail, column id of having clause is ambiguous
select item_id+user_id id, user_id+item_id as id, sum(qty) sum_qty from order_list group by item_id+user_id having id >9;


@@ sql_delete_1
-- ok
delete from user where user_name='zhang wu ji';
@@ sql_delete_2
-- fail
delete * from user where user_name='zhang wu ji';
@@ sql_delete_3
-- fail 
delete user where user_name='zhang wu ji';
@@ sql_delete_4
-- fail
delete from user as u where user_name='zhang wu ji';
@@ sql_delete_5
-- fail
delete from user, item where user.user_id = item.item_id and user_name='zhang wu ji';
@@ sql_delete_6
-- fail, Unkown column name item_name
delete from user where item_name='jiu yin zhen jing';

@@ sql_insert_1
-- ok
insert into user values (100, 'da fen qi', 15, 'a turtle');
@@ sql_insert_2
-- ok
insert into user (user_id, user_name, user_desc) values (100, 'da fen qi', 'a turtle');
@@ sql_insert_3
-- ok
insert into user (user_id, user_name, user_desc) values (100, 'da fen qi', 'a turtle'), (101, 'mi kai lang ji luo', 'another turtle'), (102, 'la fei er', 'the third turtle');
@@ sql_insert_4
-- fail, Column count doesn't match value count
insert into user (user_id, user_name, user_desc) values (100, 'da fen qi', 15, 'a turtle');
@@ sql_insert_5
-- fail
insert into user as u values (100, 'da fen qi', 15, 'a turtle');
@@ sql_insert_6
-- fail, Unkown column name item_desc
insert into user (user_id, user_name, item_desc) values (100, 'da fen qi', 'a turtle');
@@ sql_insert_7
-- ok, sytax ok, we don't check value type now
insert into user (user_id, user_name, user_desc) values ('100', 'da fen qi', 'a turtle');
@@ sql_insert_8
-- fail, num of columns and values are not match
insert into user select * from item;
@@ sql_insert_9
-- fail, must be single base table
insert into (select * from user) u values (102, 'mi lao shu');
@@ sql_insert_10
-- ok
insert into user (user_id, user_name) select item_id, item_name from item;
@@ sql_insert_11
-- ok, expression type are not check now
insert into user (user_id, user_name) select item_name, item_cost from item;
@@ sql_insert_12
-- fail, values can not be identier
insert into user values (user_id+1, user_name||'new', 45, 'have identifer');
@@ sql_insert_13
-- ok
insert into user values (89+-----1, 'new'||'name', 45, 'have identifer');
@@ sql_insert_14
-- fail
insert into user values (100, 'da fen qi', 15, 'a turtle') select * from item;
@@ sql_insert_15
-- fail, duplicate column
insert into user (user_id, user_name, user_id) values (100, 'da fen qi', 15);

@@ sql_update_1
-- fail
update item i set i.item_name='jiu gui jiu' where item_id=100;
@@ sql_update_2
-- ok
update item set item_name='jiu gui jiu' where item_id=100;
@@ sql_update_3
-- fail
update item, user set item_name='jiu gui jiu' where item_id=user_id and item_id=100;
@@ sql_update_4
-- ok
update item set item_name='jiu gui jiu', item_cost= 89, item_price=99 where item_id=(select max(item_id) from item);
@@ sql_update_5
-- ok
update item set item_name=item_name||'jiu gui jiu', item_cost= 89, item_price=99 where item_id=(select max(item_id) from item);
@@ sql_update_6
-- ok
update item set item_cost= item_price+89, item_price=item_cost--99 where item_id=100;
@@ sql_update_7
-- fail, Invalid use of group function
update item set item_cost= 89, item_price=max(item_cost) where item_id=100;
@@ sql_update_8
-- fail, value can not be subquery
update item set item_cost= (select min(item_price) from item) where item_id=100;
@@ sql_update_9
-- ok
update item set item_cost= item_name like '%jing%' where item_id=100;


@@ sql_expr_1
-- ok
select user_id, user_age, user_id+user_age from user u where u.user_name = 'deng pu fang';
@@ sql_expr_2
-- ok
select 'deng' lname, 'pu fang' as fname, lname||fname from user u where 1;
@@ sql_expr_3
-- ok
select * from user where (user_id, user_name, user_age, user_desc) in (select * from user where exists (select count(*) from item));
@@ sql_expr_4
-- fail, different columns
select * from user where (user_id, user_name, user_age) in (select * from user);
@@ sql_expr_5
-- ok
select * from user where user_id in (select user_id from order_list);
@@ sql_expr_6
-- ok
select * from user where (select count(*) from order_list);
@@ sql_expr_7
-- ok
select * from user where (user_id, user_name) in ((1, 'tangseng'), (2, 'wukong'));
@@ sql_expr_8
-- ok
select * from user where not exists (select count(*) from order_list);
@@ sql_expr_9
-- ok
select max(qty), min(qty), sum(qty), avg(qty), count(qty), count(*) from order_list;
@@ sql_expr_10
-- ok
select distinct max(distinct qty), min(distinct qty), sum(distinct qty), avg(distinct qty), count(distinct qty), count(*) from order_list;
@@ sql_expr_11
-- ok, case expr is supported now!!!
select user_id, case when qty>100 then 'winner' when qty<50 then 'loser' else 'nomal' end from order_list where item_id = 10001;
@@ sql_expr_12
-- ok
select user_id, user_name, case when user_age between 10 and 20 then 'small' when user_age between 21 and 50 then 'big' else 'illeagal' end from user;
@@ sql_expr_13
-- ok
select id, item_id, case user_id when 1001 then 'number 1' when 1002 then 'number 3' when 1003 then 'number 3' end from order_list;
@@ sql_expr_14
-- ok
select qty+6 - qty*9/(qty%25)^3 from order_list;
@@ sql_expr_15
-- ok
select * from order_list where qty is not null and order_desc is null;
@@ sql_expr_16
-- ok, qty>(10 is true) and user_id = (10086 is not true)
select * from order_list where qty>10 is true and user_id = 10086 is not true;
@@ sql_expr_17
-- ok
select * from order_list where (qty>10) is not false and (user_id = 10086) is false;
@@ sql_expr_18
-- ok
select * from order_list where order_desc is unknown;
@@ sql_expr_19
-- ok
select id||user_id||item_id||order_desc from order_list where qty between user_id and qty+user_id;
@@ sql_expr_20
-- ok
select * from order_list where not qty between 0 and 1000;
@@ sql_expr_21
-- ok
select id from order_list where order_desc like '%china%' not like 'zzzzzz';
@@ sql_expr_22
-- ok
select id from order_list where qty>=5 and qty<=9999 or order_desc is not null and order_time is not null;
@@ sql_expr_23
-- fail, In operands contain different column(s)
select (user_id, item_id, 10) in ((1, 8, 9), (2,9,6), (4, 8)) from order_list;
@@ sql_expr_24
-- ok
select (user_id, item_id, 10) in ((1, 8, 9), (2,9,6), (user_id, item_id, qty)) from order_list;
@@ sql_expr_25
-- fail, but mysql is ok!!!
select (user_id, item_id, 10) in ((1, 8, 9), (2,9,6), (select user_id, item_id, qty)) from order_list;
@@ sql_expr_26
-- fail, In operands contain different column(s)
select (user_id, item_id, 10) in ((1, 8, 9), 9, (user_id, item_id, qty)) from order_list;

@@ sql_select_1
-- ok
select * from user; select * from item;
@@ sql_select_2
-- ok
select user_id, user_name from user union all select item_id, item_name from item;
@@ sql_select_3
-- fail, num of columns are not match
select user_id, user_name from user union all select * from item;
@@ sql_select_4
-- ok
select user_id, user_name from user union select item_id, item_name from item order by user_id;
@@ sql_select_5
-- fail, order by clause gets the column name from the first select
select user_id, user_name from user union select item_id, item_name from item order by item_id;
@@ sql_select_6
-- ok
select user_id, user_name from user union select item_id, item_name from item union  select item_id, item_name from item order by user_id;
@@ sql_select_7
-- fail, just the column name no table name in the set query result
select user_id, user_name from user union select item_id, item_name from item union  select item_id, item_name from item order by user.user_id;
@@ sql_select_8
-- fail
(select user_id, user_name from user union select item_id, item_name from item) user union  select item_id, item_name from item order by user.user_id;
@@ sql_select_9
-- ok
(select user_id, user_name from user union select item_id, item_name from item) union  select item_id, item_name from item order by user_id;
@@ sql_select_10
-- ok
select user_id, user_name from user union (select item_id, item_name from item union  select item_id, item_name from item) order by user_id;
@@ sql_select_11
-- ok
select * from (select user_id, user_name from user union select item_id, item_name from item) user union  select item_id, item_name from item order by user_id;

@@ sql_order_by_1
-- ok
select user_id, sum(qty) from order_list group by user_id order by (select count(*) from item);
@@ sql_order_by_2
-- fail, Unknown column '3' in 'order clause'
select user_id, sum(qty) from order_list group by user_id order by 1 desc, 2 asc, 3 asc;
@@ sql_order_by_3
-- ok
select user_id, sum(qty) from order_list group by user_id order by item_id;
@@ sql_order_by_4
-- ok
select user_id, qty, sum(qty) as sum_qty from order_list group by user_id order by sum_qty;
@@ sql_order_by_5
-- ok, although qty is ambigous, but column of base table is come first, which is same as mysql
select user_id, qty, sum(qty) as qty from order_list group by user_id order by qty;
@@ sql_order_by_6
-- ok
select user_id, qty, sum(qty) as qty from order_list group by user_id order by item_id, max(order_time) desc;

@@ sql_limit_1
-- ok
select * from (select * from user) u, (select * from order_list) o where u.user_id=o.user_id limit 5 offset 2;
@@ sql_limit_2
-- ok
select * from (select * from user) u, (select * from order_list) o where u.user_id=o.user_id limit 5;
@@ sql_limit_3
-- ok
select * from (select * from user) u, (select * from order_list) o where u.user_id=o.user_id offset 2;
@@ sql_limit_4
-- ok
select * from (select * from user) u, (select * from order_list) o where u.user_id=o.user_id limit 5, 2;
@@ sql_limit_5
-- ok
select * from (select * from user) u, (select * from order_list) o where u.user_id=o.user_id order by qty limit 5, 2;
@@ sql_limit_6
-- fail
select * from (select * from user) u, (select * from order_list) o where u.user_id=o.user_id limit 5, 2 order by qty;
@@ sql_limit_7
-- fail, Every derived table must have its own alias
select * from (select * from user) u, (select * from order_list where user_id>0 order by qty) limit 5, 2;
