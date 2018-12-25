-- `stress_sqls' table list all sqls used to stress
stress_sqls = {
  {
    sql = [[insert into simple_table (id, number) values(?, ?);]],
    params = {
      {type = "distinct_int"},
      {type = "dependent_int", dependent_index = 1, difference = 0},
    }
  },
  {
    sql = [[replace into simple_table (id, number) values(?, ?);]],
    params = {
      {type = "int", range_min = 1, range_max = 1000000},
      {type = "dependent_int", dependent_index = 1, difference = 0},
    }
  },
  {
    sql = [[delete from simple_table where id = ?;]],
    params = {
      {type = "int", range_min = 1, range_max = 1000000},
    },
    weight=1
  },
  {
    sql = [[update simple_table set number = ? where id = ?;]],
    params = {
      {type = "dependent_int", dependent_index = 2, difference = 0},
      {type = "int", range_min = 1, range_max = 1000000},
    }
  },
  {
    sql = [[select id, number from simple_table where id = ?;]],
    params = {
      {type = "int", range_min = 1, range_max = 1000000},
    }
  },
}

