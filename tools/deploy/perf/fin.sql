[prepare]

table_num=13

table_names=fin_influx_extension;fin_influx_inst;fin_settle_serial_map;fin_influx_terminal;fin_info_transaction;fin_influx_terminal_unique;fin_request_no_unique;fin_settle_serial_unique;fin_influx_transaction;fin_influx_payer;idx1_fin_influx_payer;idx2_fin_influx_payer;idx_influxid_pay_account_no

# table 1
fin_influx_extension = influx_id(1,1);req_extension('reqqqqqqqqqqqqqqqqqqqqqq',1);res_extension('ressssssssssssssssssss',1);gmt_create(1,1);gmt_modified(1,1);extension1(1,1);extension2(1,1)

# table 2
fin_influx_inst = influx_id(1,1);inst_id(1,1);inst_serial_no(1,1);inst_ref_no(1,1);resp_amount(1,1);resp_currency('abc',1);inst_result_code('1',1);inst_result_description('result description',1);gmt_settle(1,1);gmt_resp(1,1);gmt_create(1,1);gmt_modified(1,1)

# table 3
fin_settle_serial_map = settle_serial_no(1,1);finance_exchange_code(1,1);influx_id(1,1);gmt_create(1,1);gmt_modified(1,1);inst_id(1,1)

# table 4
fin_influx_terminal = influx_id(1,1);term_trace_no(1,1);inst_id(1,1);inst_terminal_no(1,1);term_batch_no('bn',1);rrn('rrn',1);auth_code('acode',1);gmt_create(1,1);gmt_modified(1,1);finance_exchange_code(1,1);inst_account_no(1,1);exchange_amount(1,1);exchange_currency('ec',1);inst_merchant_no(1,1);extension(1,1)

# table 5
fin_info_transaction = info_id(1,1);inst_id(1,1);business_code('business code',1);sub_business_code('sub code',1);exchange_type('extype',1);finance_exchange_code(1,1);exchange_status('status',1);request_info('request_info',1);response_info('response_info',1);gmt_send(1,1);gmt_res(1,1);gmt_create(1,1);gmt_modified(1,1);result_code('rcode',1);result_description('result_desc',1);inst_result_code('in_result_code',1);inst_result_description('in_result_des',1)

# table 6
fin_influx_terminal_unique = inst_id(1,1);inst_merchant_no(1,1);inst_terminal_no(1,1);term_batch_no('bn',1);term_trace_no(1,1);gmt_create(1,1);gmt_modified(1,1);

# table 7
fin_request_no_unique = request_identify(1,1);request_biz_no(1,1);gmt_create(1,1);gmt_modified(1,1);influx_id(1,1)

# table 8
fin_settle_serial_unique = finance_exchange_code(1,1);settle_serial_no(1,1);gmt_create(1,1);gmt_modified(1,1)

# table 9


fin_influx_transaction = influx_id(1,1);org_influx_id(1,1);inst_id(1,1);business_code('business code',1);sub_business_code('sub code',1);exchange_type('extype',1);finance_exchange_code('f_extype',1);settle_serial_no(1,1);payer_account_no(1,1);exchange_amount(1,1);exchange_currency('ec',1);account_amount(1,1);account_currency('ac',1);settle_amount(1,1);settle_currency('sc',1);settle_status('ss',1);exchange_status('exs',1);result_code('rcode',1);result_description('result_desc',1);recover_flag('f',1);recon_flag('f',1);negative_flag('f',1);negative_exchange_type('netype',1);request_identify(1,1);request_biz_no('bizno',1);pay_unique_no(1,1);pay_channel_api(1,1);inst_channel_api(1,1);clear_channel('',1);biz_identity(1,1);gmt_submit(1,1);gmt_resp(1,1);gmt_settle(1,1);gmt_create(1,1);gmt_modified(1,1)

# table 10
idx_influxid_pay_account_no = payer_account_no(1,1);influx_id(1,1);gmt_create(1,1);gmt_modified(1,1)

# table 11
fin_influx_payer = influx_id(1,1);payer_account_no(1,1);payer_name(1,1);inst_account_no(1,1);inst_account_name(1,1);card_type('tp',1);card_index(1,1);issuer('issuer',1);agreement_no(1,1);certificate_type(1,1);certificate_no(1,1);mobile_phone('tel',1);pay_tool('ptl',1);bill_no(1,1);bill_type('bt',1);gmt_create(1,1);gmt_modified(1,1)

# table 12
idx1_fin_influx_payer = bill_no(1,1);payer_account_no(1,1);gmt_create(1,1);influx_id(1,1);virtual_col1(1,1)

# table 13
idx2_fin_influx_payer = influx_id(1,1);inst_account_no(1,1);gmt_create(1,1);payer_account_no(1,1);virtual_col1(1,1)


[run]

# table 1
point_query                 = select influx_id, extension1, extension2, req_extension, res_extension, gmt_create, gmt_modified from fin_influx_extension  where influx_id = ?;1,1,int,1

# table 2
range_query                 = select influx_id, inst_id, inst_serial_no, inst_ref_no, resp_amount, resp_currency, inst_result_code, inst_result_description, gmt_settle, gmt_resp, gmt_create, gmt_modified from fin_influx_inst  where influx_id = ?;1,1,int,1

# table 3
range_sum_query             = select finance_exchange_code, settle_serial_no, inst_id, influx_id, gmt_create, gmt_modified from fin_settle_serial_map where      settle_serial_no = ? and finance_exchange_code =?;1,2,int,1,int,1
range_orderby_query         = select finance_exchange_code, settle_serial_no, inst_id, influx_id, gmt_create, gmt_modified from fin_settle_serial_map where      settle_serial_no = ?;1,1,int,1
range_distinct_query        = select influx_id from fin_settle_serial_map   where finance_exchange_code in ('1','2','3')  and settle_serial_no = ?;-1,1,int,1

# table 4
point_join_query            =  select influx_id, inst_id, inst_merchant_no, inst_terminal_no, term_batch_no, term_trace_no, rrn, auth_code, gmt_create, gmt_modified, finance_exchange_code, inst_account_no, exchange_amount, exchange_currency, extension from fin_influx_terminal where influx_id=? and term_trace_no = ?;1,2,int,1,int,1

# table 5
range_join_query            =  select influx_id,inst_id, business_code, sub_business_code, exchange_type, finance_exchange_code, settle_serial_no, payer_account_no, exchange_amount, exchange_currency, account_amount, account_currency, settle_amount, settle_currency, settle_status, exchange_status, result_code, result_description, recover_flag, recon_flag, negative_flag, negative_exchange_type, request_identify, request_biz_no, pay_unique_no, pay_channel_api, inst_channel_api, clear_channel, biz_identity,gmt_submit,gmt_resp,gmt_settle, gmt_create,gmt_modified from fin_influx_transaction where payer_account_no=? and influx_id=? and gmt_create=?;1,3,int,1,int,1,int,1

# table 9
range_join_sum_query        =  select * from fin_info_transaction where info_id= ?;1,1,int,1

# table 10
range_join_orderby_query    =  select * from idx_influxid_pay_account_no where influx_id=?;1,1,int,1

range_join_distinct_query   = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=? and gmt_create=? and inst_account_no = ?;1,3,int,1,int,1,int,1
# table 11
delete                      =  select * from idx1_fin_influx_payer where bill_no=?;1,1,int,1
# table 12
#insert                      = select 1 from dual;1,0
insert                     = select * from idx2_fin_influx_payer where influx_id=? and gmt_create=? and inst_account_no=? and payer_account_no=?;1,4,int,1,int,1,int,1,int,1
replace                     = select * from idx2_fin_influx_payer where influx_id=? and gmt_create=? and inst_account_no=? and payer_account_no=?;1,4,int,1,int,1,int,1,int,1

# rest

#update_key                  = REPLACE INTO fin_influx_transaction(influx_id, org_influx_id, inst_id, business_code, sub_business_code, exchange_type, finance_exchange_code, settle_serial_no, payer_account_no, exchange_amount, exchange_currency, account_amount, account_currency, settle_amount, settle_currency, settle_status, exchange_status, result_code, result_description, recover_flag, recon_flag, negative_flag, negative_exchange_type, request_identify, request_biz_no, pay_unique_no, pay_channel_api, inst_channel_api, clear_channel, biz_identity, gmt_submit, gmt_resp, gmt_settle, gmt_create, gmt_modified) VALUES (?,0,0,'0_business code','0_sub code','0_extype','0_f_extype',0,?,0,'0_ec',0,'0_ac',0,'0_sc','0_ss','0_exs','0_rcode','0_result_desc','0_f','0_f','0_f','0_netype',0,'0_bizno',0,0,0,'0_',0,0,0,0,?,0);3,int,1,int,1,int,1
#update_key = select 1 from dual;1,0
update_key = select * from idx2_fin_influx_payer where influx_id=? and gmt_create=? and inst_account_no=? and payer_account_no=?;1,4,int,1,int,1,int,1,int,1
#update_nonkey               = select 1 from dual;1,0
update_nonkey = select * from idx2_fin_influx_payer where influx_id=? and gmt_create=? and inst_account_no=? and payer_account_no=?;1,4,int,1,int,1,int,1,int,1

[cleanup]

drop = drop table fin_influx_extension,fin_influx_inst,fin_settle_serial_map,fin_influx_terminal,fin_info_transaction,fin_influx_terminal_unique,fin_request_no_unique,fin_settle_serial_unique,fin_influx_transaction,fin_influx_payer,idx1_fin_influx_payer,idx2_fin_influx_payer,idx_influxid_pay_account_no;

