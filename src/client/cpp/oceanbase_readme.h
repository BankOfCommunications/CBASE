/**
 * (C) 2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * oceanbase.h : API of OceanBase
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */

/*! \mainpage OceanBase C API 说明

<div align="right">
<em>"API design is like sex: Make one mistake<br>
and support it for the rest of your life."</em><br>
<em>- \@joshbloch</em>
</div>

\section intro_sec OceanBase C API 简介

OceanBase C API提供用户读写OceanBase的方法。
同时支持同步和异步调用。

\section install_sec OceanBase C API 安装

<ol>
<li>下载<a href="libobapi-1.1.0.tar.gz">tar压缩包</a>或者
<a href="oceanbase-dev-1.1.0-0.x86_64.rpm">rpm安装包</a>。
</li>

<li>\verbatim tar -xzvf libobapi-1.1.0.tar.gz \endverbatim
或者\verbatim rpm -i oceanbase-dev-1.1.0-1.x86_64.rpm \endverbatim<br>
（安装rpm包需要root权限。）<br>
（开发包内两个主要文件oceanbase.h是接口头文件，
libobapi.so.1.1.0是动态库。）<br>
（libobapi.tar.gz会将这两个文件及符号链接加压到
当前目录，运行时需将当前目录添加到LD_LIBRARY_PATH环境变量。
obapi还依赖于libaio库，请在相应的平台上安装。
例如，在ubuntu 11.04下，执行
sudo apt-get install libaio-dev；在Red Hat下，执行
yum install libaio-devel）
</li>

</ol>

\section get_started_sec OceanBase C API 起步

Set操作示例程序（<a href="set_sync_demo.c">下载</a>）
@include set_sync_demo.c

编译
\verbatim gcc -o set_sync_demo set_sync_demo.c -lobapi -L. \endverbatim

运行
\verbatim LD_LIBRARY_PATH=.libs ./set_sync_demo
set succ \endverbatim

\section usage_sec OceanBase C API 使用基本流程

使用OceanBase C API接口访问OceanBase，程序需要使用固定的框架。
下面分为“同步接口”和“异步接口”分别进行说明。

\subsection sync_usage_sec 同步接口
<ol>
<li>调用\ref ob_api_init函数初始化OceanBase C API库。函数成功调用后，
会返回一个有效的指向\ref OB *结构体的指针；
</li>
<li>调用\ref ob_connect函数连接OceanBase，ob_connect函数需要传入
OceanBase RootServer地址与端口，和用户名与密码。返回值是\ref OB_ERR_CODE
表示成功与否；
</li>
<li>调用\ref ob_acquire_set_st或\ref ob_acquire_get_st
或\ref ob_acquire_scan_st函数 * 获得一个进行SET，GET或者SCAN的句柄；
</li>
<li>调用\ref set_func_sec或\ref get_func_sec或\ref scan_func_sec里的
SET，GET，SCAN相关函数，构造所需要的请求；
</li>
<li>调用\ref ob_exec_set或\ref ob_exec_get或\ref ob_exec_scan
执行所需要的操作；
</li>调用\ref ob_release_set_st或\ref ob_release_get_st
或\ref ob_release_scan_st或\ref ob_release_res_st释放对应的句柄；
<li>调用\ref ob_api_destroy函数释放C API库。
</li>
</ol>

\subsection async_usage_sec 异步接口
<ol>
<li>调用\ref ob_api_init函数初始化OceanBase C API库。函数成功调用后，
会返回一个有效的指向\ref OB *结构体的指针；
</li>
<li>调用\ref ob_connect函数连接OceanBase，ob_connect函数需要传入
OceanBase RootServer地址与端口，和用户名与密码。返回值是\ref OB_ERR_CODE
表示成功与否；
</li>
<li>调用\ref ob_acquire_set_req或\ref ob_acquire_get_req
或\ref ob_acquire_scan_req函数 * 获得一个进行SET，GET或者SCAN的异步句柄；
</li>
<li>调用\ref set_func_sec或\ref get_func_sec或\ref scan_func_sec里的
SET，GET，SCAN相关函数，构造所需要的请求；
</li>
<li>调用\ref ob_submit提交异步操作；
<li>调用\ref ob_get_results等待和获取异步操作结果；
</li>调用\ref ob_release_req释放异步句柄；
<li>调用\ref ob_api_destroy函数释放C API库。
</li>
</ol>

\section datatype_sec OceanBase 数据类型

OceanBase存储的是结构化数据，数据是以表格的形式组织起来的，
表格的每一列有固定的类型。

OceanBase目前支持的3种数据类型，分别是<b>OB_INT</b>（整数），
<b>OB_VARCHAR</b>（变长字符串）和<b>OB_DATETIME</b>（时间）。
用户存入OceanBase和从OceanBase中读取的数据都必须是这些类型。

<ul>
  <li>\ref OB_INT</li>

    <b>OB_INT</b>表示64位有符号整型数。
    在C语言中，OB_INT等同于int64_t（在64位机器中，long）类型，
    可以直接进行操作。

  <li>\ref OB_VARCHAR</li>

    <b>OB_VARCHAR</b>表示变长字符，有<b>p</b>和<b>len</b>两个域。
    <b>p</b>指向字符串的起始位置，<b>len</b>表示字符串的长度。
    <b>p指向的位置不保证在len+1位置有'\\0'结束符</b>，使用时需要注意。

  <li>\ref OB_DATETIME</li>

    <b>OB_DATETIME</b>表示日期。
    在C语言中，OB_DATETIME是一个结构体，
    等同于sys/time.h头文件中的timeval类型，
    包含<b>tv_sec</b>和<b>tv_usec</b>两个域，
    <b>tv_sec</b>表示秒，<b>tv_usec</b>表示微秒。
    通常在OceanBase中，OB_DATETIME表示的是从
    Epoch (00:00:00 UTC, January 1, 1970)到要表示的时间所经历的时间。

</ul>

\section const_sec OceanBase C API 常量

<ul>

  <li>\ref OB_TYPE</li>
    表示的是OceanBase支持的数据类型对应的常量，
    对应于OB_INT, OB_VARCHAR和OB_DATETIME三种数据类型，
    分别有OB_INT_TYPE, OB_VARCHAR_TYPE和OB_DATETIME_TYPE三个常量。
    <ul>
      <li>OB_INT_TYPE</li>
      <li>OB_VARCHAR_TYPE</li>
      <li>OB_DATETIME_TYPE</li>
    </ul>

  <li>\ref OB_OPERATION_CODE</li>
    使用异步方式操作OB时，异步控制结构体opcode域用来存储操作类型。
    OB_OPERATION_CODE枚举型表示操作OceanBase的类型：
    <ul>
      <li>OB_OP_GET: 读取一个或多个cell</li>
      <li>OB_OP_SCAN: 扫描</li>
      <li>OB_OP_SET: 修改</li>
    </ul>

  <li>\ref OB_ORDER</li>
    从OceanBase扫描数据时，可以指定按照指定列排序。
    OB_ORDER枚举型定义的排序的方向：
    <ul>
      <li>OB_ASC: 升序</li>
      <li>OB_DESC: 降序</li>
    </ul>

  <li>\ref OB_AGGREGATION_TYPE</li>
    从OceanBase扫描数据时，可以对某些列进行聚合操作，
    等同于SQL语法中的COUNT, SUM等操作。
    OB_AGGREGATION_TYPE枚举型定义了这些操作的常量：
    <ul>
      <li>OB_SUM: 求和</li>
      <li>OB_COUNT: 计数</li>
      <li>OB_MAX: 最大值</li>
      <li>OB_MIN: 最小值</li>
    </ul>

  <li>\ref OB_LOGIC_OPERATOR</li>
    OB_SET操作允许指定修改条件，即当指定条件满足时才执行操作，否则不执行。
    这些条件通过OB_SET_COND结构指定，
    OB_LOGIC_OPERATOR枚举型定义了条件中需要用到的操作常量：
    <ul>
      <li>OB_LT: less than 小于</li>
      <li>OB_LE: less than or equal 小于等于</li>
      <li>OB_EQ: equal 等于</li>
      <li>OB_GT: greater than 大于</li>
      <li>OB_GE: greater than or equal 大于等于</li>
      <li>OB_NE: not equal 不等于</li>
      <li>OB_LIKE: 子串</li>
    </ul>

  <li>\ref OB_REQ_FLAG</li>
    OB异步接口允许用户指定一个fd，当异步请求完成时，通过这个fd来通知用户。
    OB_REQ结构中，通过resfd域用来记录进行操作的fd，
    flags域里用来指定需要进行这个操作。
    当flags域等于<b>FD_INVOCATION</b>时，当异步完成时，会通过fd来通知。

</ul>

\section struct_sec OceanBase C API 数据结构

<ul>
  <li>\ref OB</li>

  <li>\ref OB_VALUE</li>

  <li>\ref OB_CELL</li>

  <li>\ref OB_SET</li>

  <li>\ref OB_SCAN</li>

  <li>\ref OB_GET</li>

  <li>\ref OB_RES</li>

  <li>\ref OB_SET_COND</li>

  <li>\ref OB_GROUPBY_PARAM</li>

  <li>\ref OB_REQ</li>

</ul>

\section func_sec OceanBase C API 函数
\subsection submit_func_sec API基础函数
<ul>
  <li>\ref ob_api_init</li>
  <li>\ref ob_api_destroy</li>
  <li>\ref ob_connect</li>
  <li>\ref ob_close</li>
  <li>\ref ob_exec_scan</li>
  <li>\ref ob_exec_get</li>
  <li>\ref ob_exec_set</li>
  <li>\ref ob_submit</li>
  <li>\ref ob_cancel</li>
  <li>\ref ob_get_results</li>
  <li>\ref ob_acquire_get_req</li>
  <li>\ref ob_acquire_scan_req</li>
  <li>\ref ob_acquire_set_req</li>
  <li>\ref ob_release_req</li>
  <li>\ref ob_acquire_get_st</li>
  <li>\ref ob_acquire_scan_st</li>
  <li>\ref ob_acquire_set_st</li>
  <li>\ref ob_release_get_st</li>
  <li>\ref ob_release_scan_st</li>
  <li>\ref ob_release_set_st</li>
  <li>\ref ob_reset_get_st</li>
  <li>\ref ob_reset_scan_st</li>
  <li>\ref ob_reset_set_st</li>
  <li>\ref ob_release_res_st</li>
  <li>\ref ob_req_set_resfd</li>
  <li>\ref ob_api_cntl</li>
</ul>

\subsection err_func_sec 错误信息相关函数
<ul>
  <li>\ref ob_errno</li>
  <li>\ref ob_error</li>
  <li>\ref ob_get_err</li>
  <li>\ref ob_set_errno</li>
  <li>\ref ob_set_error</li>
  <li>\ref ob_set_err</li>
</ul>

\subsection set_func_sec 插入操作相关函数
<ul>
  <li>\ref ob_update</li>
  <li>\ref ob_update_int</li>
  <li>\ref ob_update_varchar</li>
  <li>\ref ob_update_datetime</li>
  <li>\ref ob_insert</li>
  <li>\ref ob_insert_int</li>
  <li>\ref ob_insert_varchar</li>
  <li>\ref ob_insert_datetime</li>
  <li>\ref ob_accumulate_int</li>
  <li>\ref ob_del_row</li>
  <li>\ref ob_get_ob_set_cond</li>
  <li>\ref ob_cond_add</li>
  <li>\ref ob_cond_add_int</li>
  <li>\ref ob_cond_add_varchar</li>
  <li>\ref ob_cond_add_datetime</li>
  <li>\ref ob_cond_add_exist</li>
</ul>

\subsection get_func_sec Get操作相关函数
<ul>
  <li>\ref ob_get_cell</li>
</ul>

\subsection scan_func_sec Scan操作相关函数
<ul>
  <li>\ref ob_scan</li>
  <li>\ref ob_scan_column</li>
  <li>\ref ob_scan_complex_column</li>
  <li>\ref ob_scan_set_where</li>
  <li>\ref ob_scan_add_simple_where</li>
  <li>\ref ob_scan_add_simple_where_int</li>
  <li>\ref ob_scan_add_simple_where_varchar</li>
  <li>\ref ob_scan_add_simple_where_datetime</li>
  <li>\ref ob_scan_orderby_column</li>
  <li>\ref ob_scan_set_limit</li>
  <li>\ref ob_scan_set_precision</li>
  <li>\ref ob_get_ob_groupby_param</li>
  <li>\ref ob_groupby_column</li>
  <li>\ref ob_aggregate_column</li>
  <li>\ref ob_groupby_add_complex_column</li>
  <li>\ref ob_groupby_add_return_column</li>
  <li>\ref ob_groupby_set_having</li>
  <li>\ref ob_scan_res_join_append</li>
</ul>

\subsection res_func_sec 结果操作相关函数
<ul>
  <li>\ref ob_fetch_cell</li>
  <li>\ref ob_fetch_row</li>
  <li>\ref ob_res_seek_to_begin_cell</li>
  <li>\ref ob_res_seek_to_begin_row</li>
  <li>\ref ob_fetch_row_num</li>
  <li>\ref ob_fetch_whole_res_row_num</li>
  <li><i>\ref ob_is_fetch_all_res (deprecated)</i></li>
</ul>

\subsection value_func_sec value值操作函数
<ul>
  <li>\ref ob_set_value_int</li>
  <li>\ref ob_set_value_varchar</li>
  <li>\ref ob_set_value_datetime</li>
</ul>

\subsection debug_func_sec debug相关函数
<ul>
  <li>\ref ob_get_ms_list</li>
  <li>\ref ob_api_debug_log</li>
  <li>\ref ob_debug</li>
</ul>

\section scan_tutorial OceanBase C API Scan操作说明

首先建立了一个表格，然后针对这个表格进行一系列的查询。
表格记录了Los、San、Bos三个商店的每日销售汇总状况，
其中sales表示商店当日销售总额，counts表示商店当日售出的总物品数。

下面所有查询结果都是在Mysql中实验得到的。表格信息如下：
\verbatim
mysql> select * from store_information;
+------------+-------+--------+------------+
| store_name | sales | counts | sale_date  |
+------------+-------+--------+------------+
| Los        |  1500 |      1 | 2000-01-03 |
| Los        |   300 |      1 | 2000-01-01 |
| San        |   250 |      2 | 2000-01-01 |
| Bos        |   700 |      3 | 2000-01-05 |
+------------+-------+--------+------------+
\endverbatim

对应这条SQL查询的OB API的Scan接口的调用方法如下：
\code
OB_SCAN* scan_st = acquire_scan_st(ob);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 1);
ob_scan_column(scan_st, "counts", 1);
ob_scan_column(scan_st, "sale_date", 1);
\endcode

查询每个商店的总营业额：
\verbatim
select store_name, sum(sales) from store_information group by store_name;
+------------+------------+
| store_name | sum(sales) |
+------------+------------+
| Bos        |        700 |
| Los        |       1800 |
| San        |        250 |
+------------+------------+
\endverbatim

这条SQL语句对应的OB API的Scan接口调用方式如下：

\code
OB_SCAN *scan_st = acquire_scan_st(ob);
OB_GROUPBY_PARAM *groupby_st = ob_get_ob_groupby_param(scan_st);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 0);
ob_groupby_column(groupby_st, "store_name", 1);
ob_aggregate_column(groupby_st, OB_SUM, "sales", "s", 1);
\endcode

查询总营业额大于1500的商店
\verbatim
select store_name, sum(sales) from store_information group by store_name having sum(sales) > 1500;
+------------+------------+
| store_name | sum(sales) |
+------------+------------+
| Los        |       1800 |
+------------+------------+
\endverbatim

\code
OB_SCAN *scan_st = acquire_scan_st(ob);
OB_GROUPBY_PARAM *groupby_st = ob_get_ob_groupby_param(scan_st);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 0);
ob_groupby_column(groupby_st, "store_name", 1);
ob_aggregate_column(groupby_st, OB_SUM, "sales", "s", 1);
ob_groupby_set_having(groupby_st, "s > 1500");
\endcode

查询售出总物件数等于2的的商店及其总营业额

\verbatim
select store_name, sum(sales) from store_information group by store_name having sum(counts) = 2;
+------------+------------+
| store_name | sum(sales) |
+------------+------------+
| Los        |       1800 |
| San        |        250 |
+------------+------------+
\endverbatim

\code
OB_SCAN *scan_st = acquire_scan_st(ob);
OB_GROUPBY_PARAM *groupby_st = ob_get_ob_groupby_param(scan_st);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 0);
ob_scan_column(scan_st, "counts", 0);
ob_groupby_column(groupby_st, "store_name", 1);
ob_aggregate_column(groupby_st, OB_SUM, "sales", "s", 1);
ob_aggregate_column(groupby_st, OB_SUM, "counts", "c", 0);  
ob_groupby_set_having(groupby_st, "c = 2");
\endcode


查询售出总物件数等于2的的商店及其平均每个物件的售价

\verbatim
select store_name, sum(sales)/sum(counts) as sale from store_information group by store_name having sum(counts) = 2;
+------------+----------+
| store_name | sale     |
+------------+----------+
| Los        | 900.0000 |
| San        | 125.0000 |
+------------+----------+
\endverbatim

\code
OB_SCAN *scan_st = acquire_scan_st(ob);
OB_GROUPBY_PARAM *groupby_st = ob_get_ob_groupby_param(scan_st);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 0);
ob_scan_column(scan_st, "counts", 0);
ob_groupby_column(groupby_st, "store_name", 1);
ob_aggregate_column(groupby_st, OB_SUM, "sales", "s", 0);
ob_aggregate_column(groupby_st, OB_SUM, "counts", "c", 0);  
ob_groupby_add_complex_column(groupby_st, "s / c", "sale", 1);
ob_groupby_set_having(groupby_st, "c = 2");
\endcode


查询2000-01-03前售出总物件数等于2的的商店及其平均每个物件的售价

\verbatim
select store_name, sum(sales)/sum(counts) as sale from store_information where sale_date < '2000-01-03' group by store_name having sum(counts) = 2;
+------------+----------+
| store_name | sale     |
+------------+----------+
| San        | 125.0000 |
+------------+----------+
\endverbatim

\code
OB_SCAN *scan_st = acquire_scan_st(ob);
OB_GROUPBY_PARAM *groupby_st = ob_get_ob_groupby_param(scan_st);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 0);
ob_scan_column(scan_st, "counts", 0);
ob_scan_column(scan_st, "sale_date", 0);
ob_scan_set_where(scan_st, "sale_date < #2000-01-03#");
ob_groupby_column(groupby_st, "store_name", 1);
ob_aggregate_column(groupby_st, OB_SUM, "sales", "s", 0);
ob_aggregate_column(groupby_st, OB_SUM, "counts", "c", 0);  
ob_groupby_add_complex_column(groupby_st, "s / c", "sale", 1);
ob_groupby_set_having(groupby_st, "c = 2");
\endcode


查询2000-01-03前售出了物品的商店及其每日平均每个物件的售价

\verbatim
select store_name, sales/counts as sale from store_information where sale_date < '2000-01-03';
+------------+----------+
| store_name | sale     |
+------------+----------+
| Los        | 300.0000 |
| San        | 125.0000 |
+------------+----------+
\endverbatim

\code
OB_SCAN *scan_st = acquire_scan_st(ob);
OB_GROUPBY_PARAM *groupby_st = ob_get_ob_groupby_param(scan_st);
ob_scan_column(scan_st, "store_name", 1);
ob_scan_column(scan_st, "sales", 0);
ob_scan_column(scan_st, "counts", 0);
ob_scan_column(scan_st, "sale_date", 0);
ob_scan_complex_column(scan_st, "sales / counts", "sale", 1);
ob_scan_set_where(scan_st, "sale_date < #2000-01-03#");
\endcode


\section async_sec OceanBase C API 异步操作方案

\section demo_sec OceanBase C API 示例
\subsection set_sync_demo_sec OceanBase C API 同步Set操作示例
@include set_sync_demo.c

\subsection scan_sync_demo_sec OceanBase C API 同步Scan操作示例
@include scan_sync_demo.c

\subsection set_async_demo_sec OceanBase C API 异步Set操作示例
@include set_async_demo.c

\subsection scan_async_demo_sec OceanBase C API 异步Scan操作示例
@include scan_async_demo.c

\subsection groupby_async_demo_sec OceanBase C API 异步Group By操作示例
@include sum_async_demo.c

 */

