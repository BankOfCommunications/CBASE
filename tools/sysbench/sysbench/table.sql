## table 1
create table fin_influx_extension(
influx_id varchar(64) primary key,
extension1 varchar(32),
extension2 varchar(32),
req_extension varchar(4000),
res_extension varchar(4000),
gmt_create datetime,
gmt_modified datetime);

## table 2
create table fin_influx_inst(
influx_id varchar(64) primary key,
inst_id varchar(32),
inst_serial_no varchar(32),
inst_ref_no varchar(32),
resp_amount int,
resp_currency char(3),
inst_result_code  varchar(16),
inst_result_description varchar(256),
gmt_settle datetime,
gmt_resp datetime,
gmt_create datetime,
gmt_modified datetime);

## table 3 fin_settle_serial_map
create table fin_settle_serial_map(
influx_id varchar(64),
settle_serial_no varchar(64),
inst_id varchar(32),
finance_exchange_code varchar(32),
gmt_create datetime,
gmt_modified datetime,
primary key(settle_serial_no, finance_exchange_code)
);

## table 4 fin_influx_terminal
create table fin_influx_terminal(
influx_id      varchar(64),
inst_id       varchar(32),
inst_merchant_no   varchar(32),
inst_terminal_no  varchar(32),
term_batch_no      varchar(6),
term_trace_no       varchar(6),
rrn varchar(12),
auth_code  varchar(6),
gmt_create  datetime,
gmt_modified datetime,
finance_exchange_code varchar(32),
inst_account_no    varchar(32),
exchange_amount    int,
exchange_currency  char(3),
extension     varchar(4000),
primary key(influx_id, term_trace_no)
);

## table 5 fin_info_transaction
create table fin_info_transaction(
info_id varchar(64) primary key,
inst_id varchar(32),
business_code   varchar(16),
sub_business_code       varchar(16),
exchange_type   varchar(32),
finance_exchange_code   varchar(32),
exchange_status varchar(8),
request_info    varchar(4000),
response_info   varchar(4000),
gmt_send        datetime,
gmt_res datetime,
gmt_create      datetime,
gmt_modified    datetime,
result_code     varchar(16),
result_description      varchar(256),
inst_result_code        varchar(16),
inst_result_description varchar(256)
);

## table 6 fin_influx_terminal_unique
create table fin_influx_terminal_unique(
inst_id varchar(32),
inst_merchant_no        varchar(32),
inst_terminal_no        varchar(32),
term_batch_no   varchar(6),
term_trace_no   varchar(6),
gmt_create      datetime,
gmt_modified    datetime,
primary key(inst_id, term_batch_no, inst_merchant_no, term_trace_no, inst_terminal_no)
);


## table 7 fin_request_no_unique
create table fin_request_no_unique(
request_identify        varchar(32),
request_biz_no  varchar(64),
gmt_create      datetime,
gmt_modified    datetime,
influx_id       varchar(64),
primary key(request_identify, request_biz_no)
);


## table 8 fin_settle_serial_unique
create table fin_settle_serial_unique(
finance_exchange_code   varchar(32),
settle_serial_no        varchar(64),
gmt_create      datetime,
gmt_modified    datetime,
primary key(finance_exchange_code, settle_serial_no)
);

## table 9 fin_influx_transaction
create table fin_influx_transaction(
influx_id       varchar(64),
org_influx_id   varchar(64),
inst_id varchar(32),
business_code   varchar(16),
sub_business_code       varchar(16),
exchange_type   varchar(32),
finance_exchange_code   varchar(32),
settle_serial_no        varchar(64),
payer_account_no        varchar(32),
exchange_amount int,
exchange_currency       char(3),
account_amount  int,
account_currency        char(3),
settle_amount   int,
settle_currency char(3),
settle_status   varchar(8),
exchange_status varchar(8),
result_code     varchar(16),
result_description      varchar(256),
recover_flag    char(1),
recon_flag      char(1),
negative_flag   char(1),
negative_exchange_type  varchar(16),
request_identify        varchar(32),
request_biz_no  varchar(64),
pay_unique_no   varchar(64),
pay_channel_api varchar(32),
inst_channel_api        varchar(32),
clear_channel   varchar(32),
biz_identity    varchar(32),
gmt_submit      datetime,
gmt_resp        datetime,
gmt_settle      datetime,
gmt_create      datetime,
gmt_modified    datetime,
primary key(payer_account_no, gmt_create, influx_id)
);

### table 10 idx_influxid_pay_account_no

create table idx_influxid_pay_account_no(
payer_account_no        varchar(32),
gmt_create datetime,
gmt_modified datetime,
influx_id       varchar(64),
primary key(influx_id)
);


### table 11 fin_influx_payer

create table fin_influx_payer(

influx_id       varchar(64) ,
payer_account_no        varchar(32),
payer_name      varchar(32),
inst_account_no varchar(32),
inst_account_name       varchar(128),

card_type       varchar(16),
card_index      varchar(32),
issuer  varchar(32),
agreement_no    varchar(100),
certificate_type        varchar(64),
certificate_no  varchar(32),
mobile_phone    varchar(16),
pay_tool        varchar(16),
bill_no varchar(32),
bill_type       varchar(8),
gmt_create      datetime,
gmt_modified    datetime,
primary key(inst_account_no, gmt_create, influx_id)
);



## table 12 idx1_fin_influx_payer
create table idx1_fin_influx_payer (
bill_no varchar(32),
payer_account_no varchar(32),
gmt_create      datetime,
influx_id       varchar(64),
virtual_col1    int,
primary key(bill_no,payer_account_no,gmt_create,influx_id)
);
# bill_no+inst_account_no+gmt_create+influx_id+influx_id

## table 13 idx2_fin_influx_payer
create table idx2_fin_influx_payer(
inst_account_no varchar(32),
gmt_create      datetime,
payer_account_no varchar(32),
influx_id       varchar(64),
virtual_col1    int,
primary key(inst_account_no,payer_account_no,gmt_create,influx_id)
);
