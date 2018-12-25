-- `stress_sqls' table list all sqls used to stress
stress_sqls = {
  {
    sql = "select id, number from simple_table where id=?;",
    params = {
      {type = "int", range_min = 1, range_max = 100},
    },
    weight = 1
  },
  {
    sql = "select /*+ read_static */ id, number from simple_table where id>=? and id <= ?;",
    params = {
      {type = "int", range_min = 101, range_max = 200},
      {type = "dependent_int", dependent_index = 1, difference = 60},
    },
    weight = 4
  },
}

prepare_sqls = {
  {
    sql = "replace into simple_table values(?,?);",
    params = {
      {type = "int", range_min = 1, range_max = 100},
      {type = "int", range_min = 1, range_max = 100},
      gen_type = "parallel_loop"  -- two options "parallel_loop" and "nested_loop"
    },
  },
  {
    sql = "replace into simple_table values(?,?);",
    params = {
      {type = "int", range_min = 101, range_max = 200},
      {type = "int", range_min = 101, range_max = 200},
      gen_type = "parallel_loop"  -- two options "parallel_loop" and "nested_loop"
    },
  },
}
