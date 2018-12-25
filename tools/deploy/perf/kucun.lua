-- `stress_sqls' table list all sqls used to stress
stress_sqls = {
  {
    sql = [[
insert into ipm_inventory_detail ( opt_type, opt_num, inventory_id, user_id, item_id, sku_id, bizorderid, childbizorderid,
  quantity, gmt_create, gmt_modified, status, version, time_out, time_number, opt_gmt_create, area_id, feature, flag,
  store_code, reserved_by_cache, store_quantity, reserved_quantity )
  values( 901, ?, 229470000034651912, 734912947, 16189925503, 25981667911, 228580756797565, 228580756807565, 1, current_time(),
  current_time(), 1, 0,  TIMESTAMP'2013-02-24 14:24:16', 0, TIMESTAMP'2013-02-24 14:07:16', 440608, null, 2, 'IC', 0, 31, 1 ) 
when 
  row_count(update ipm_inventory set version=version+1, gmt_modified=current_time(), quantity = 100009070, reserve_quantity = 
  reserve_quantity + 1 where item_id = ? and sku_id = 0 and type = 901 and store_code = 'IC' and (100009070 - reserve_quantity - 1) >= 0) = 1;
]],
    params = {
      {type = "distinct_int"},
      {type = "int", range_min = 1, range_max = 20},
    }
  },
}

