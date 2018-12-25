-- `stress_sqls' table list all sqls used to stress
stress_sqls = {
  {
    sql = [[
select id, number from simple_table where id=?;
]],
    params = {
      {type = "int", range_min = 1, range_max = 40000},
    },
    weight = 1
  },
}
--/*+ read_static */ 
--
--prepare_sqls = {
--  {
--    sql = [[
--replace into simple_table values(?,?);
--]],
--    params = {
--      {type = "int", range_min = 1, range_max = 40000},
--      {type = "int", range_min = 1, range_max = 40000},
--      gen_type = "parallel_loop"
--    },
--    weight = 1
--  },
--}
prepare_sqls = {
  {
    sql = [[
replace into simple_table values(?,?);
]],
    params = {
      {type = "int", range_min = 1, range_max = 40000},
      {type = "int", range_min = 1, range_max = 40000},
      gen_type = "parallel_loop"
    },
  },
}
