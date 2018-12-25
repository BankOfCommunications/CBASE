[prepare]

table_num=1

table_names=fin_influx_payer

# table fin_influx_payer
fin_influx_payer = influx_id(1,1);payer_account_no(1,1);payer_name(1,1);inst_account_no(1,1);inst_account_name(1,1);card_type('tp',1);card_index(1,1);issuer('issuer',1);agreement_no(1,1);certificate_type(1,1);certificate_no(1,1);mobile_phone('tel',1);pay_tool('ptl',1);bill_no(1,1);bill_type('bt',1);gmt_create(1,1);gmt_modified(1,1)

[run]

point_query                 = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_query                 = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_sum_query             = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_orderby_query         = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_distinct_query        = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
point_join_query            = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_join_query            = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_join_sum_query        = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_join_orderby_query    = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
range_join_distinct_query   = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
delete                      = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
insert                      = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
replace                     = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
update_key                  = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1
update_nonkey               = select influx_id, payer_account_no, payer_name, inst_account_no, inst_account_name, card_type, card_index, issuer, agreement_no, certificate_type, certificate_no, mobile_phone, pay_tool, bill_no, bill_type, gmt_create, gmt_modified from fin_influx_payer   where influx_id=?;1,1,int,1

[cleanup]

drop = drop table fin_influx_payer;

