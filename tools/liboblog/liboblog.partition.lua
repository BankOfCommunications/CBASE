
--database partition
function ipm_inventory_db_partition(...)
  split = arg[1];
  --split = string.sub(split, -4, -1);
  partition = split % 16;
  return partition;
end
function ipm_inventory_detail_db_partition(...)
  split = arg[1];
  --split = string.sub(split, -4, -1);
  partition = split % 16;
  return partition;
end

--table partition
function ipm_inventory_tb_partition(...)
  split = arg[1];
  --split = string.sub(split, -4, -1);
  partition = split % 4096;
  return partition;
end
function ipm_inventory_detail_tb_partition(...)
  split = arg[1];
  --split = string.sub(split, -4, -1);
  partition = split % 4096;
  return partition;
end

