package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.CollectColumnEnum;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Value;

public class InsertTestCaseExecuteForJoin extends BaseCase {

    // private String []CollectColumns =
    // collectInfoTable.getAllColumnNames().toArray(new String[0]);
    private String[] CollectJoinColumns = collectInfoTable.getJoinColumnNames().toArray(new String[0]);

    private String[] CollectNonJoinColumns = collectInfoTable.getNonJoinColumnNames().toArray(new String[0]);

    private String[] CollectInsertColumns = setInsertColumnsforCollect().toArray(new String[0]);

    private String[] ItemColumns = itemInfoTable.getAllColumnNames().toArray(new String[0]);

    public Set<String> setInsertColumnsforCollect() {
        Set<String> Columns = collectInfoTable.getNonJoinColumnNames();
        Columns.remove("gm_modified");
        Columns.remove("gm_create");
        return Columns;
    }
    public Set<String> setInsertColumnsforItem() {
        Set<String> Columns = itemInfoTable.getNonJoinColumnNames();
        Columns.remove("gm_modified");
        Columns.remove("gm_create");
        return Columns;
    }

    public Set<String> setOutofOrderColumnsforCollectNonJoin() {
        Set<String> Columns = new HashSet<String>();
        Columns.add(CollectInsertColumns[7]);
        Columns.add(CollectInsertColumns[0]);
        Columns.add(CollectInsertColumns[4]);
        Columns.add(CollectInsertColumns[6]);
        Columns.add(CollectInsertColumns[3]);
        Columns.add(CollectInsertColumns[1]);
        Columns.add(CollectInsertColumns[5]);
        Columns.add(CollectInsertColumns[2]);
        return Columns;
    }

    public Set<String> setOutofOrderColumnsforCollectJoin() {
        Set<String> Columns = new HashSet<String>();
        Columns.add(CollectJoinColumns[7]);
        Columns.add(CollectJoinColumns[0]);
        Columns.add(CollectJoinColumns[10]);
        Columns.add(CollectJoinColumns[6]);
        Columns.add(CollectJoinColumns[3]);
        Columns.add(CollectJoinColumns[9]);
        Columns.add(CollectJoinColumns[5]);
        Columns.add(CollectJoinColumns[2]);
        Columns.add(CollectJoinColumns[8]);
        Columns.add(CollectJoinColumns[4]);
        Columns.add(CollectJoinColumns[1]);
        return Columns;
    }

    public Set<String> setOutofOrderColumnsforItem() {
        Set<String> Columns = new HashSet<String>();
        Columns.add(ItemColumns[7]);
        Columns.add(ItemColumns[0]);
        Columns.add(ItemColumns[10]);
        Columns.add(ItemColumns[6]);
        Columns.add(ItemColumns[3]);
        Columns.add(ItemColumns[9]);
        Columns.add(ItemColumns[5]);
        Columns.add(ItemColumns[2]);
        Columns.add(ItemColumns[8]);
        Columns.add(ItemColumns[4]);
        Columns.add(ItemColumns[1]);
        return Columns;
    }

    public InsertMutator getNormalIMforCollect(String table, RKey rowkey, Set<String> columns, int times) {
        InsertMutator mutator = new InsertMutator(table, rowkey);
        for (String str : columns) {
            mutator.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rowkey, times, false));
        }
        return mutator;
    }

    public InsertMutator getNormalIMforItem(String table, RKey rowkey, Set<String> columns, int times) {
        InsertMutator mutator = new InsertMutator(table, rowkey);
        for (String str : columns) {
            mutator.addColumn(str, itemInfoTable.genColumnUpdateResult(str, rowkey, times, false));
        }
        return mutator;
    }

    public InsertMutator getAbnormalIMforCollectNonJoin(String table, RKey rowkey, int Order) {
        InsertMutator mutator = new InsertMutator(table, rowkey);
        if (Order == 7 || Order == 23) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                Value value = new Value();
                String s = CollectInsertColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("CollectNonJoinInsert");
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("double")) {
                    value.setDouble(Order * (double) 1);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("float")) {
                    value.setFloat(Order * 1.0f);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("long")) {
                    value.setNumber(Order);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("time")) {
                    value.setSecond(Order);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("precisetime")) {
                    value.setMicrosecond(Order);
                    mutator.addColumn(s, value);
                }
            }
        } else if (Order == 11 || Order == 12 || Order == 26) {
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                if (i == 1 || i == 4 || i == 7) {
                    if (Order == 11)
                        mutator.addColumn(null,
                                collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
                    else if (Order == 12)
                        mutator.addColumn("",
                                collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
                    else {
                        Value value = new Value();
                        mutator.addColumn(null, value);
                    }
                } else
                    mutator.addColumn(CollectInsertColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
            }
        } else if (Order == 13) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectNonJoinColumns.length; i++) {
                Value value = new Value();
                String s = CollectNonJoinColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("time")) {
                    if (temp.getValue().equals("gm_create") || temp.getValue().equals("gm_modified"))
                        mutator.addColumn(s, value);
                    else
                        mutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, 0, false));
                } else
                    mutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, 0, false));
            }
        } else if (Order == 14) {
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                Value value = new Value();
                String s = CollectInsertColumns[i];
                mutator.addColumn(s, value);
            }
        } else if (Order == 15) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                Value value = new Value();
                String s = CollectInsertColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("CollectNonJoinInsert");
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("double")) {
                    value.setDouble(Double.MIN_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("float")) {
                    value.setFloat(Float.MAX_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("long")) {
                    value.setNumber(Long.MAX_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("time")) {
                    value.setSecond(Long.MIN_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("precisetime")) {
                    value.setMicrosecond(Long.MIN_VALUE);
                    mutator.addColumn(s, value);
                }
            }
        } else if (Order == 18) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                Value value = new Value();
                String s = CollectInsertColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("");
                    mutator.addColumn(s, value);
                } else
                    mutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, 0, false));
            }
        } else if (Order == 19) {
            Value value = new Value();
            value.setString("NotMatch");
            mutator.addColumn(CollectInsertColumns[0], value);
            for (int i = 1; i < CollectInsertColumns.length; i++) {
                mutator.addColumn(CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
            }
        } else if (Order == 20) {
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                Value value = new Value();
                if (i == 1) {
                    value.setDouble(Order / 100);
                    mutator.addColumn(CollectInsertColumns[i], value);
                } else if (i == 4) {
                    value.setNumber(Order);
                    mutator.addColumn(CollectInsertColumns[i], value);
                } else if (i == 7) {
                    value.setString("NotMatch");
                    mutator.addColumn(CollectInsertColumns[i], value);
                } else
                    mutator.addColumn(CollectInsertColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
            }
        } else if (Order == 28) {
            for (int i = 0; i < CollectInsertColumns.length; i++)
                mutator.addColumn(CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
            mutator.addColumn(CollectInsertColumns[CollectInsertColumns.length - 1], collectInfoTable
                    .genColumnUpdateResult(CollectInsertColumns[CollectInsertColumns.length - 1], rowkey, 28, false));
        } else if (Order == 29) {
            for (int i = 0; i < 3; i++)
                mutator.addColumn(CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
            mutator.addColumn(CollectInsertColumns[2],
                    collectInfoTable.genColumnUpdateResult(CollectInsertColumns[2], rowkey, 29, false));
            for (int i = 3; i < CollectInsertColumns.length; i++)
                mutator.addColumn(CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false));
        } else if (Order == 30) {
            mutator.addColumn(CollectInsertColumns[0],
                    collectInfoTable.genColumnUpdateResult(CollectInsertColumns[0], rowkey, 0, false));
            for (int i = 1; i < CollectInsertColumns.length; i++) {
                Value value = new Value();
                mutator.addColumn(CollectInsertColumns[i], value);
            }
        }

        return mutator;
    }

    public InsertMutator getAbnormalIMforCollectJoin(String table, RKey rowkey, int Order) {
        InsertMutator mutator = new InsertMutator(table, rowkey);
        if (Order == 7 || Order == 23) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectJoinColumns.length; i++) {
                Value value = new Value();
                String s = CollectJoinColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("CollectJoinInsert");
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("double")) {
                    value.setDouble(Order * (double) 1);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("float")) {
                    value.setFloat(Order * 1.0f);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("long")) {
                    value.setNumber(Order);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("time")) {
                    value.setSecond(Order);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("precisetime")) {
                    value.setMicrosecond(Order);
                    mutator.addColumn(s, value);
                }
            }
        } else if (Order == 11 || Order == 12 || Order == 26) {
            for (int i = 0; i < CollectJoinColumns.length; i++) {
                if (i == 1 || i == 4 || i == 7) {
                    if (Order == 11)
                        mutator.addColumn(null,
                                collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
                    else if (Order == 12)
                        mutator.addColumn("",
                                collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
                    else {
                        Value value = new Value();
                        mutator.addColumn(null, value);
                    }
                } else
                    mutator.addColumn(CollectJoinColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
            }
        } else if (Order == 14) {
            for (int i = 0; i < CollectJoinColumns.length; i++) {
                Value value = new Value();
                String s = CollectJoinColumns[i];
                mutator.addColumn(s, value);
            }
        } else if (Order == 15) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectJoinColumns.length; i++) {
                Value value = new Value();
                String s = CollectJoinColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("CollectJoinInsert");
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("double")) {
                    value.setDouble(Double.MIN_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("float")) {
                    value.setFloat(Float.MAX_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("long")) {
                    value.setNumber(Long.MAX_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("time")) {
                    value.setSecond(Long.MIN_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("precisetime")) {
                    value.setMicrosecond(Long.MIN_VALUE);
                    mutator.addColumn(s, value);
                }
            }
        } else if (Order == 18) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectJoinColumns.length; i++) {
                Value value = new Value();
                String s = CollectJoinColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("");
                    mutator.addColumn(s, value);
                } else
                    mutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, 0, false));
            }
        } else if (Order == 19) {
            Value value = new Value();
            value.setString("NotMatch");
            mutator.addColumn(CollectJoinColumns[0], value);
            for (int i = 1; i < CollectJoinColumns.length; i++) {
                mutator.addColumn(CollectJoinColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
            }
        } else if (Order == 20) {
            for (int i = 0; i < CollectJoinColumns.length; i++) {
                Value value = new Value();
                if (i == 1) {
                    value.setDouble(Order / 100);
                    mutator.addColumn(CollectJoinColumns[i], value);
                } else if (i == 4) {
                    value.setNumber(Order);
                    mutator.addColumn(CollectJoinColumns[i], value);
                } else if (i == 8) {
                    value.setString("NotMatch");
                    mutator.addColumn(CollectJoinColumns[i], value);
                } else
                    mutator.addColumn(CollectJoinColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
            }
        } else if (Order == 28) {
            for (int i = 0; i < CollectJoinColumns.length; i++)
                mutator.addColumn(CollectJoinColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
            mutator.addColumn(CollectJoinColumns[CollectJoinColumns.length - 1], collectInfoTable
                    .genColumnUpdateResult(CollectJoinColumns[CollectJoinColumns.length - 1], rowkey, 28, false));
        } else if (Order == 29) {
            for (int i = 0; i < 3; i++)
                mutator.addColumn(CollectJoinColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
            mutator.addColumn(CollectJoinColumns[2],
                    collectInfoTable.genColumnUpdateResult(CollectJoinColumns[2], rowkey, 29, false));
            for (int i = 3; i < CollectJoinColumns.length; i++)
                mutator.addColumn(CollectJoinColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false));
        } else if (Order == 30) {
            mutator.addColumn(CollectJoinColumns[0],
                    collectInfoTable.genColumnUpdateResult(CollectJoinColumns[0], rowkey, 0, false));
            for (int i = 1; i < CollectJoinColumns.length; i++) {
                Value value = new Value();
                mutator.addColumn(CollectJoinColumns[i], value);
            }
        }

        return mutator;
    }

    public InsertMutator getAbnormalIMforItem(String table, RKey rowkey, int Order) {
        InsertMutator mutator = new InsertMutator(table, rowkey);
        if (Order == 7 || Order == 23) {
            CollectColumnEnum temp;
            for (int i = 0; i < ItemColumns.length; i++) {
                Value value = new Value();
                String s = ItemColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("ItemInsert");
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("double")) {
                    value.setDouble(Order * (double) 1);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("float")) {
                    value.setFloat(Order * 1.0f);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("long")) {
                    value.setNumber(Order);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("time")) {
                    value.setSecond(Order);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("precisetime")) {
                    value.setMicrosecond(Order);
                    mutator.addColumn(s, value);
                }
            }
        } else if (Order == 11 || Order == 12 || Order == 26) {
            for (int i = 0; i < ItemColumns.length; i++) {
                if (i == 1 || i == 4 || i == 7) {
                    if (Order == 11)
                        mutator.addColumn(null, itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
                    else if (Order == 12)
                        mutator.addColumn("", itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
                    else {
                        Value value = new Value();
                        mutator.addColumn(null, value);
                    }
                } else
                    mutator.addColumn(ItemColumns[i],
                            itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
            }
        } else if (Order == 14) {
            for (int i = 0; i < ItemColumns.length; i++) {
                Value value = new Value();
                String s = ItemColumns[i];
                mutator.addColumn(s, value);
            }
        } else if (Order == 15) {
            CollectColumnEnum temp;
            for (int i = 0; i < ItemColumns.length; i++) {
                Value value = new Value();
                String s = ItemColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("ItemInsert");
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("double")) {
                    value.setDouble(Double.MIN_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("float")) {
                    value.setFloat(Float.MAX_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("long")) {
                    value.setNumber(Long.MAX_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("time")) {
                    value.setSecond(Long.MIN_VALUE);
                    mutator.addColumn(s, value);
                } else if (temp.getType().equals("precisetime")) {
                    value.setMicrosecond(Long.MIN_VALUE);
                    mutator.addColumn(s, value);
                }
            }
        } else if (Order == 18) {
            CollectColumnEnum temp;
            for (int i = 0; i < ItemColumns.length; i++) {
                Value value = new Value();
                String s = ItemColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    value.setString("");
                    mutator.addColumn(s, value);
                } else
                    mutator.addColumn(s, itemInfoTable.genColumnUpdateResult(s, rowkey, 0, false));
            }
        } else if (Order == 19) {
            Value value = new Value();
            value.setString("NotMatch");
            mutator.addColumn(ItemColumns[0], value);
            for (int i = 1; i < ItemColumns.length; i++) {
                mutator.addColumn(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
            }
        } else if (Order == 20) {
            for (int i = 0; i < ItemColumns.length; i++) {
                Value value = new Value();
                if (i == 1) {
                    value.setDouble(Order / 100);
                    mutator.addColumn(ItemColumns[i], value);
                } else if (i == 4) {
                    value.setNumber(Order);
                    mutator.addColumn(ItemColumns[i], value);
                } else if (i == 7) {
                    value.setString("NotMatch");
                    mutator.addColumn(ItemColumns[i], value);
                } else
                    mutator.addColumn(ItemColumns[i],
                            itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
            }
        } else if (Order == 28) {
            for (int i = 0; i < ItemColumns.length; i++)
                mutator.addColumn(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
            mutator.addColumn(ItemColumns[ItemColumns.length - 1],
                    itemInfoTable.genColumnUpdateResult(ItemColumns[ItemColumns.length - 1], rowkey, 28, false));
        } else if (Order == 29) {
            for (int i = 0; i < 3; i++)
                mutator.addColumn(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
            mutator.addColumn(ItemColumns[2], itemInfoTable.genColumnUpdateResult(ItemColumns[2], rowkey, 29, false));
            for (int i = 3; i < ItemColumns.length; i++)
                mutator.addColumn(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false));
        } else if (Order == 30) {
            mutator.addColumn(ItemColumns[0], itemInfoTable.genColumnUpdateResult(ItemColumns[0], rowkey, 0, false));
            for (int i = 1; i < ItemColumns.length; i++) {
                Value value = new Value();
                mutator.addColumn(ItemColumns[i], value);
            }
        }

        return mutator;
    }

    private void insertCheckforCollect(Result<RowData> result, RKey rowkey, int Order) {
        if (Order == 15) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                String s = CollectInsertColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    assertEquals(s, "CollectNonJoinInsert", result.getResult().get(s));
                } else if (temp.getType().equals("double")) {
                    assertEquals(s, Double.MIN_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("float")) {
                    assertEquals(s, Float.MAX_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("long")) {
                    assertEquals(s, Long.MAX_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("time")) {
                    assertEquals(s, Long.MIN_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("precisetime")) {
                    assertEquals(s, Long.MIN_VALUE, result.getResult().get(s));
                }
            }
        } else if (Order == 18) {
            CollectColumnEnum temp;
            for (int i = 0; i < CollectInsertColumns.length; i++) {
                String s = CollectInsertColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    assertEquals(s, "", result.getResult().get(s));
                } else
                    assertEquals(s, collectInfoTable.genColumnUpdateResult(s, rowkey, 0, false).getObject(false)
                            .getValue(), result.getResult().get(s));
            }
        } else if (Order == 28) {
            for (int i = 0; i < CollectInsertColumns.length - 1; i++)
                assertEquals(
                        CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false)
                                .getObject(false).getValue(), result.getResult().get(CollectInsertColumns[i]));
            assertEquals(
                    CollectInsertColumns[CollectInsertColumns.length - 1],
                    collectInfoTable
                            .genColumnUpdateResult(CollectInsertColumns[CollectInsertColumns.length - 1], rowkey, 28,
                                    false).getObject(false).getValue(),
                    result.getResult().get(CollectInsertColumns[CollectInsertColumns.length - 1]));
        } else if (Order == 29) {
            for (int i = 0; i < 2; i++)
                assertEquals(
                        CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false)
                                .getObject(false).getValue(), result.getResult().get(CollectInsertColumns[i]));
            assertEquals(CollectInsertColumns[2],
                    collectInfoTable.genColumnUpdateResult(CollectInsertColumns[2], rowkey, 29, false).getObject(false)
                            .getValue(), result.getResult().get(CollectInsertColumns[2]));
            for (int i = 3; i < CollectInsertColumns.length; i++)
                assertEquals(
                        CollectInsertColumns[i],
                        collectInfoTable.genColumnUpdateResult(CollectInsertColumns[i], rowkey, 0, false)
                                .getObject(false).getValue(), result.getResult().get(CollectInsertColumns[i]));
        } else if (Order == 30) {
            assertEquals(CollectInsertColumns[0],
                    collectInfoTable.genColumnUpdateResult(CollectInsertColumns[0], rowkey, 0, false).getObject(false)
                            .getValue(), result.getResult().get(CollectInsertColumns[0]));
            for (int i = 1; i < CollectInsertColumns.length; i++)
                assertEquals(CollectInsertColumns[i], null, result.getResult().get(CollectInsertColumns[i]));
        }
    }

    private void insertCheckforItem(Result<RowData> result, RKey rowkey, int Order, boolean flag) {
        if (Order == 15) {
            CollectColumnEnum temp;
            for (int i = 0; i < ItemColumns.length; i++) {
                String s = null;
                if (flag)
                    s = ItemColumns[i];
                else
                    s = CollectJoinColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    assertEquals("ItemInsert", result.getResult().get(s));
                } else if (temp.getType().equals("double")) {
                    assertEquals(Double.MIN_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("float")) {
                    assertEquals(Float.MAX_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("long")) {
                    assertEquals(Long.MAX_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("time")) {
                    assertEquals(Long.MIN_VALUE, result.getResult().get(s));
                } else if (temp.getType().equals("precisetime")) {
                    assertEquals(Long.MIN_VALUE, result.getResult().get(s));
                }
            }
        } else if (Order == 18) {
            CollectColumnEnum temp;
            for (int i = 0; i < ItemColumns.length; i++) {
                String s = null;
                if (flag)
                    s = ItemColumns[i];
                else
                    s = CollectJoinColumns[i];
                temp = CollectColumnEnum.valueOf(s.toUpperCase());
                if (temp.getType().equals("string")) {
                    assertEquals(s, "", result.getResult().get(s));
                } else {
                    if (flag)
                        assertEquals(s, itemInfoTable.genColumnUpdateResult(s, rowkey, 0, false).getObject(false)
                                .getValue(), result.getResult().get(s));
                    else
                        assertEquals(s, collectInfoTable.genColumnUpdateResult(s, rowkey, 0, false).getObject(false)
                                .getValue(), result.getResult().get(s));
                }
            }
        } else if (Order == 28) {
            if (flag) {
                for (int i = 0; i < ItemColumns.length - 1; i++)
                    assertEquals(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false)
                            .getObject(false).getValue(), result.getResult().get(ItemColumns[i]));
                assertEquals(ItemColumns[ItemColumns.length - 1],
                        itemInfoTable.genColumnUpdateResult(ItemColumns[ItemColumns.length - 1], rowkey, 28, false)
                                .getObject(false).getValue(),
                        result.getResult().get(ItemColumns[ItemColumns.length - 1]));
            } else {
                for (int i = 0; i < CollectJoinColumns.length - 2; i++)
                    assertEquals(
                            CollectJoinColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false)
                                    .getObject(false).getValue(), result.getResult().get(CollectJoinColumns[i]));
                assertEquals(
                        CollectJoinColumns[CollectJoinColumns.length - 2],
                        collectInfoTable
                                .genColumnUpdateResult(CollectJoinColumns[CollectJoinColumns.length - 2], rowkey, 28,
                                        false).getObject(false).getValue(),
                        result.getResult().get(CollectJoinColumns[CollectJoinColumns.length - 2]));
                assertEquals(
                        CollectJoinColumns[CollectJoinColumns.length - 1],
                        collectInfoTable
                                .genColumnUpdateResult(CollectJoinColumns[CollectJoinColumns.length - 1], rowkey, 0,
                                        false).getObject(false).getValue(),
                        result.getResult().get(CollectJoinColumns[CollectJoinColumns.length - 1]));
            }
        } else if (Order == 29) {
            if (flag) {
                for (int i = 0; i < 2; i++)
                    assertEquals(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false)
                            .getObject(false).getValue(), result.getResult().get(ItemColumns[i]));
                assertEquals(ItemColumns[2], itemInfoTable.genColumnUpdateResult(ItemColumns[2], rowkey, 29, false)
                        .getObject(false).getValue(), result.getResult().get(ItemColumns[2]));
                for (int i = 3; i < ItemColumns.length; i++)
                    assertEquals(ItemColumns[i], itemInfoTable.genColumnUpdateResult(ItemColumns[i], rowkey, 0, false)
                            .getObject(false).getValue(), result.getResult().get(ItemColumns[i]));
            } else {
                for (int i = 0; i < 2; i++)
                    assertEquals(
                            CollectJoinColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false)
                                    .getObject(false).getValue(), result.getResult().get(CollectJoinColumns[i]));
                assertEquals(
                        CollectJoinColumns[2],
                        collectInfoTable.genColumnUpdateResult(CollectJoinColumns[2], rowkey, 29, false)
                                .getObject(false).getValue(), result.getResult().get(CollectJoinColumns[2]));
                for (int i = 3; i < CollectJoinColumns.length; i++)
                    assertEquals(
                            CollectJoinColumns[i],
                            collectInfoTable.genColumnUpdateResult(CollectJoinColumns[i], rowkey, 0, false)
                                    .getObject(false).getValue(), result.getResult().get(CollectJoinColumns[i]));
            }
        } else if (Order == 30) {
            if (flag) {
                assertEquals(ItemColumns[0], itemInfoTable.genColumnUpdateResult(ItemColumns[0], rowkey, 0, false)
                        .getObject(false).getValue(), result.getResult().get(ItemColumns[0]));
                for (int i = 1; i < ItemColumns.length; i++)
                    assertEquals(ItemColumns[i], null, result.getResult().get(ItemColumns[i]));
            } else {
                assertEquals(CollectJoinColumns[0],
                        collectInfoTable.genColumnUpdateResult(CollectJoinColumns[0], rowkey, 0, false)
                                .getObject(false).getValue(), result.getResult().get(CollectJoinColumns[0]));
                for (int i = 1; i < CollectJoinColumns.length; i++)
                    assertEquals(CollectJoinColumns[i], null, result.getResult().get(CollectJoinColumns[i]));
            }
        }

    }

    @Before
    public void setUp() throws Exception {
        obClient.clearActiveMemTale();
    }

    @After
    public void tearDown() throws Exception {
        obClient.clearActiveMemTale();
    }

    @Test
    public void test_insert_01_HappyPath() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 1));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        for (String str : ColumnsforCollectInsert) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false)
                    .getValue(), rdforCollect1.getResult().get(str));
        }

        InsertMutator imforCollect2 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames(), 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 1));
        InsertMutator imforItem = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames(), 0);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false)
                    .getValue(), rdforItem.getResult().get(str));
        }
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false)
                    .getValue(), rdforCollect3.getResult().get(str));
        }
    }

    @Test
    public void test_insert_02_DataExist() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 2));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert, 1);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        for (String str : ColumnsforCollectInsert) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 1, false).getObject(false)
                    .getValue(), rdforCollect1.getResult().get(str));
        }

        InsertMutator imforCollect2 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames(), 1);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), rdforCollect2.getResult()
                    .get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '0', 2));
        InsertMutator imforItem = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames(), 1);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 1, false).getObject(false)
                    .getValue(), rdforItem.getResult().get(str));
        }
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 1, false).getObject(false)
                    .getValue(), rdforCollect3.getResult().get(str));
        }
        RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte) '0', 2));
        Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect2, 1, false).getObject(false)
                    .getValue(), rdforCollect4.getResult().get(str));
        }
    }

    @Test
    public void test_insert_03_tableIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 3));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        try {
            InsertMutator imforCollect1 = getNormalIMforCollect(null, rowkeyForCollect, ColumnsforCollectInsert, 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getNormalIMforCollect(null, rowkeyForCollect,
                    collectInfoTable.getJoinColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 3));
        try {
            InsertMutator imforItem = getNormalIMforItem(null, rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_04_tableIsBlank() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 4));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        try {
            InsertMutator imforCollect1 = getNormalIMforCollect("", rowkeyForCollect, ColumnsforCollectInsert, 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getNormalIMforCollect("", rowkeyForCollect,
                    collectInfoTable.getJoinColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 4));
        try {
            InsertMutator imforItem = getNormalIMforItem("", rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_06_rowkeyIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = null;
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        try {
            InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                    ColumnsforCollectInsert, 0);
        } catch (IllegalArgumentException e) {
            assertEquals("rowkey is null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                    collectInfoTable.getJoinColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("rowkey is null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = null;
        try {
            InsertMutator imforItem = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem,
                    itemInfoTable.getAllColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("rowkey is null", e.getMessage());
        }
    }

    @Test
    public void test_insert_07_rowkeyIsBlank() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new byte[] {});
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                7);
        try {
            obClient.insert(imforCollect1);
        } catch (IllegalArgumentException e) {
            assertEquals("rowkey bytes null", e.getMessage());
        }

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 7);
        try {
            obClient.insert(imforCollect2);
        } catch (IllegalArgumentException e) {
            assertEquals("rowkey bytes null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new byte[] {});
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 7);
        try {
            obClient.insert(imforItem);
        } catch (IllegalArgumentException e) {
            assertEquals("rowkey bytes null", e.getMessage());
        }
    }

    @Test
    public void test_insert_09_ColumnsIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 9));
        InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
        try {
            obClient.insert(imforCollect);
        } catch (IllegalArgumentException e) {
            assertEquals("columns null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 9));
        InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
        try {
            obClient.insert(imforItem);
        } catch (IllegalArgumentException e) {
            assertEquals("columns null", e.getMessage());
        }
    }

    @Test
    public void test_insert_11_partColumnsNameIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 11));
        try {
            InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(),
                    rowkeyForCollect, 11);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(),
                    rowkeyForCollect, 11);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 11));
        try {
            InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 11);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_12_partColumnsNameIsBlank() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 12));
        try {
            InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(),
                    rowkeyForCollect, 12);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(),
                    rowkeyForCollect, 12);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 12));
        try {
            InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 12);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_13_ForbidNullColumnIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 13));
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                13);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertFalse("Insert success!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getNonJoinColumnNames());
        assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
		assertTrue("No records find!", rdforCollect1.getResult().isEmpty());    }

    @Test
    public void test_insert_14_NotForbidNullColumnIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 14));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                14);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        for (String str : ColumnsforCollectInsert) {
            assertEquals(str, null, rdforCollect1.getResult().get(str));
        }

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 14);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 14));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 14);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, null, rdforItem.getResult().get(str));
        }
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect3.getResult().get(str));
        }
    }

    @Test
    public void test_insert_15_NumberColumnIsBoundaryValue() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 15));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                15);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        insertCheckforCollect(rdforCollect1, rowkeyForCollect, 15);

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 15);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 15));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 15);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        insertCheckforItem(rdforItem, rowkeyForItem, 15, true);
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        insertCheckforItem(rdforCollect3, rowkeyForCollect, 15, false);
    }

    @Test
    public void test_insert_18_valueIsBlank() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 18));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                18);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        insertCheckforCollect(rdforCollect1, rowkeyForCollect, 18);

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 18);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 18));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 18);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        insertCheckforItem(rdforItem, rowkeyForItem, 18, true);
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        insertCheckforItem(rdforCollect3, rowkeyForCollect, 18, false);
    }

    @Test
    public void test_insert_19_ValueNotMatchType() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 19));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                19);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertFalse("Insert success!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        assertEquals(ResultCode.OB_SUCCESS,rdforCollect1.getCode());
		assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 19);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        assertEquals(ResultCode.OB_SUCCESS,rdforCollect2.getCode());
		assertTrue("No records find!", rdforCollect2.getResult().isEmpty());

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 19));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 19);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertFalse("Insert success!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        assertEquals(ResultCode.OB_SUCCESS,rdforItem.getCode());
		assertTrue("No records find!", rdforItem.getResult().isEmpty());        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        assertEquals(ResultCode.OB_SUCCESS, rdforCollect3.getCode());
		assertTrue("No records find!", rdforCollect3.getResult().isEmpty());    }

   

    @Test
    public void test_insert_20_MultiValueNotMatchType() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 20));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                20);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertFalse("Insert success!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        assertEquals(ResultCode.OB_SUCCESS,rdforCollect1.getCode());
		assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 20);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        assertEquals(ResultCode.OB_SUCCESS,rdforCollect2.getCode());
		assertTrue("No records find!", rdforCollect2.getResult().isEmpty());

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 20));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 20);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertFalse("Insert success!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        assertEquals(ResultCode.OB_SUCCESS, rdforItem.getCode());
		assertTrue("No records find!", rdforItem.getResult().isEmpty()); 

        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        assertEquals(ResultCode.OB_SUCCESS,rdforCollect3.getCode());
		assertTrue("No records find!", rdforCollect3.getResult().isEmpty());    
		}



    @Test
    public void test_insert_23_tableIsNullAndRowkeyIsBlank() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new byte[] {});
        try {
            InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(null, rowkeyForCollect, 23);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(null, rowkeyForCollect, 23);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new byte[] {});
        try {
            InsertMutator imforItem = getAbnormalIMforItem(null, rowkeyForItem, 23);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_24_tableIsBlankAndRowkeyIsNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = null;
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        try {
            InsertMutator imforCollect1 = getNormalIMforCollect("", rowkeyForCollect, ColumnsforCollectInsert, 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getNormalIMforCollect("", rowkeyForCollect,
                    collectInfoTable.getJoinColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = null;
        try {
            InsertMutator imforItem = getNormalIMforItem("", rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        } catch (IllegalArgumentException e) {
            assertEquals("table name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_25_ColumnsIsOutofOrder() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 25));
        Set<String> OutofOrderColumnsforCollectInsert = setOutofOrderColumnsforCollectNonJoin();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                OutofOrderColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                OutofOrderColumnsforCollectInsert);
        for (String str : OutofOrderColumnsforCollectInsert) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false)
                    .getValue(), rdforCollect1.getResult().get(str));
        }

        Set<String> OutofOrderColumnsforCollectJoin = setOutofOrderColumnsforCollectJoin();
        InsertMutator imforCollect2 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,
                OutofOrderColumnsforCollectJoin, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 25));
        Set<String> OutofOrderColumnsforItem = setOutofOrderColumnsforItem();
        InsertMutator imforItem = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem,
                OutofOrderColumnsforItem, 0);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false)
                    .getValue(), rdforItem.getResult().get(str));
        }
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false)
                    .getValue(), rdforCollect3.getResult().get(str));
        }
    }

    @Test
    public void test_insert_26_NameAndValueAllNullNoType() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 26));
        try {
            InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(),
                    rowkeyForCollect, 26);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }

        try {
            InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(),
                    rowkeyForCollect, 26);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 26));
        try {
            InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 26);
        } catch (IllegalArgumentException e) {
            assertEquals("column name null", e.getMessage());
        }
    }

    @Test
    public void test_insert_28_LastColumnRepeat() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 28));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                28);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        insertCheckforCollect(rdforCollect1, rowkeyForCollect, 28);

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 28);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 28));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 28);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        insertCheckforItem(rdforItem, rowkeyForItem, 28, true);
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        insertCheckforItem(rdforCollect3, rowkeyForCollect, 28, false);
    }

    @Test
    public void test_insert_29_MidColumnRepeat() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 29));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                29);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        insertCheckforCollect(rdforCollect1, rowkeyForCollect, 29);

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 29);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 29));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 29);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        insertCheckforItem(rdforItem, rowkeyForItem, 29, true);
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        insertCheckforItem(rdforCollect3, rowkeyForCollect, 29, false);
    }

    @Test
    public void test_insert_30_OneColumnIsnotNull() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 30));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect,
                30);
        Result<Boolean> rsforCollect1 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect1.getResult());
        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);
        insertCheckforCollect(rdforCollect1, rowkeyForCollect, 30);

        InsertMutator imforCollect2 = getAbnormalIMforCollectJoin(collectInfoTable.getTableName(), rowkeyForCollect, 30);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect2);
        assertFalse("Insert success!", rsforCollect2.getResult());
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, null, rdforCollect2.getResult().get(str));
        }

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 30));
        InsertMutator imforItem = getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 30);
        Result<Boolean> rsforItem = obClient.insert(imforItem);
        assertTrue("Insert fail!", rsforItem.getResult());
        Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem,
                itemInfoTable.getAllColumnNames());
        insertCheckforItem(rdforItem, rowkeyForItem, 30, true);
        Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                collectInfoTable.getJoinColumnNames());
        insertCheckforItem(rdforCollect3, rowkeyForCollect, 30, false);
    }

    @Test
    public void test_insert_31_StringFieldMoreThan256() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 31));
        InsertMutator insertMutatorForNonInfo = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);

        Value valueInfo = new Value();
        String newString = "abcdefghij";
        String insertStringMoreThan256 = "a";
        for (int i = 0; i < 27; i++) {
            insertStringMoreThan256 = insertStringMoreThan256.concat(newString);
        }
        valueInfo.setString(insertStringMoreThan256);
        insertMutatorForNonInfo.addColumn(CollectNonJoinColumns[4], valueInfo);// user_nick
        Result<Boolean> rsforCollect1 = obClient.insert(insertMutatorForNonInfo);
        assertEquals(false, rsforCollect1.getResult());

        /* ItemInfo */
        RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte) '1', 31));
        InsertMutator insertMutatorForItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
        Value valueItem = new Value();
        valueItem.setString(insertStringMoreThan256);
        insertMutatorForItem.addColumn(ItemColumns[1], valueItem);// title
        Result<Boolean> rsforItem = obClient.insert(insertMutatorForItem);
        assertEquals(false, rsforItem.getResult());
    }

    // can not find the critical value
    @Test
    public void test_insert_32_CollectTimeMoreThanLimit() {
        /* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '1', 32));
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator insertMutatorForNonInfo = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
        long collectTime = Long.parseLong("742057594037927936");// 72057594037927936

        Value valueInfo = new Value();
        valueInfo.setSecond(collectTime);
        insertMutatorForNonInfo.addColumn(CollectNonJoinColumns[9], valueInfo);// collect_time
        Result<Boolean> rsforCollect1 = obClient.insert(insertMutatorForNonInfo);

        assertEquals(true, rsforCollect1.getResult());

        Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect,
                ColumnsforCollectInsert);

        assertEquals(collectTime, rdforCollect1.getResult().get(CollectNonJoinColumns[9]));
    }
    
    //@Test
    public void test_insert_33_ItemNotInsertOneCollectJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 33));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
        
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 33));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(null, rdforCollect1.getResult());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
       
        // get one join column
        for (String str : collectInfoTable.getJoinColumnNames()) {
        	Set<String> Columns = new HashSet<String>();
            Columns.add(str);
            Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
            log.debug(str + " should be " + collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue() + ", actually is " + rdforCollect2.getResult().get(str) );
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    @Test
    public void test_insert_34_ItemInsertOneCollectJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 34));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 34));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
		assertTrue("No records find!", rdforCollect1.getResult().isEmpty());        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
        /* ItemInfo */
        InsertMutator imforItem1 = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        Result<Boolean> rsforItem1 = obClient.insert(imforItem1);
        assertTrue("Insert fail!", rsforItem1.getResult());
        Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem1.getResult().get(str));
        }
        
        // get one join column
        for (String str : collectInfoTable.getJoinColumnNames()) {
        	Set<String> Columns = new HashSet<String>();
            Columns.add(str);
            Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    //@Test
    public void test_insert_35_ItemNotInsertTwoCollectJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 35));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
        
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 35));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(null, rdforCollect1.getResult());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
       
        // get two join columns
        Set<String> Columns = new HashSet<String>();
        Columns.add(CollectJoinColumns[3]);
        Columns.add(CollectJoinColumns[7]);
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
        for (String str : Columns) {
        	assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    @Test
    public void test_insert_36_ItemInsertTwoCollectJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 36));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 36));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
		assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
        /* ItemInfo */
        InsertMutator imforItem1 = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        Result<Boolean> rsforItem1 = obClient.insert(imforItem1);
        assertTrue("Insert fail!", rsforItem1.getResult());
        Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem1.getResult().get(str));
        }
        
        // get two join columns
        Set<String> Columns = new HashSet<String>();
        Columns.add(CollectJoinColumns[3]);
        Columns.add(CollectJoinColumns[7]);
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
        for (String str : Columns) {
        	assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    //@Test
    public void test_insert_37_ItemNotInsertAllCollectJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 37));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
        
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 37));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(null, rdforCollect1.getResult());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
       
        // get all join columns
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    @Test
    public void test_insert_38_ItemInsertAllCollectJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 38));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 38));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
	    assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
        /* ItemInfo */
        InsertMutator imforItem1 = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        Result<Boolean> rsforItem1 = obClient.insert(imforItem1);
        assertTrue("Insert fail!", rsforItem1.getResult());
        Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem1.getResult().get(str));
        }
        
        // get all join columns
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
        for (String str : collectInfoTable.getJoinColumnNames()) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    //@Test
    public void test_insert_39_ItemNotInsertCollectOneJoinOneNonJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 39));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
        
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 39));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(null, rdforCollect1.getResult());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
       
        // get one join column, one nonjoin column
        Set<String> Columns = new HashSet<String>();
        Columns.add(CollectNonJoinColumns[9]);
        Columns.add(CollectJoinColumns[10]);
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
        for (String str : Columns) {
        	assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    @Test
    public void test_insert_40_ItemInsertCollectOneJoinOneNonJoinColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 40));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 40));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
	    assertTrue("No records find!", rdforCollect1.getResult().isEmpty());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
        /* ItemInfo */
        InsertMutator imforItem1 = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        Result<Boolean> rsforItem1 = obClient.insert(imforItem1);
        assertTrue("Insert fail!", rsforItem1.getResult());
        Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem1.getResult().get(str));
        }
        
        // get one join column, one nonjoin column
        Set<String> Columns = new HashSet<String>();
        Columns.add(CollectNonJoinColumns[9]);
        Columns.add(CollectJoinColumns[10]);
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
        for (String str : Columns) {
        	assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    //@Test
    public void test_insert_41_ItemNotInsertAllCollectColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
        
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 41));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(null, rdforCollect1.getResult());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
       
        // get all columns
        Set<String> Columns = collectInfoTable.getAllColumnNames();
        Columns.remove("gm_modified");
        Columns.remove("gm_create");
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
        for (String str : Columns) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
    @Test
    public void test_insert_42_ItemInsertAllCollectColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 42));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 42));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
	    assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
        /* ItemInfo */
        InsertMutator imforItem1 = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames(), 0);
        Result<Boolean> rsforItem1 = obClient.insert(imforItem1);
        assertTrue("Insert fail!", rsforItem1.getResult());
        Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem1.getResult().get(str));
        }
        
        // get all columns
        Set<String> Columns = collectInfoTable.getAllColumnNames();
        Columns.remove("gm_modified");
        Columns.remove("gm_create");
        Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
        for (String str : Columns) {
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    } 
    
    @Test
    public void test_insert_43_ItemNotInsertAllCollectColumns() {
    	/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 43));
/*		Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
        for (String str : itemInfoTable.getAllColumnNames()) {
            assertEquals(str, itemInfoTable.genColumnUpdateResult(str, rowkeyForItem, 0, false).getObject(false).getValue(), rdforItem.getResult().get(str));
        }*/
        
    	/* CollectInfo */
        RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte) '0', 43));
        		
        // delete 
        Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
		assertTrue("Delete fail!", rsforCollect1.getResult());
		Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
		assertTrue("No records find!", rdforCollect1.getResult().isEmpty());	
        // insert 
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect, ColumnsforCollectInsert, 0);
        Result<Boolean> rsforCollect2 = obClient.insert(imforCollect1);
        assertTrue("Insert fail!", rsforCollect2.getResult());
        
        //insert item
        Set<String> ColumnsforItemInsert = setInsertColumnsforItem();
        InsertMutator imforItem1 = getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, ColumnsforItemInsert, 0);
        Result<Boolean> rsforItem2 = obClient.insert(imforItem1);
        assertTrue("Insert fail!", rsforItem2.getResult());
       
        // get one join column
        for (String str : collectInfoTable.getJoinColumnNames()) {
        	Set<String> Columns = new HashSet<String>();
            Columns.add(str);
            Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns);
            log.debug(str + " should be " + collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue() + ", actually is " + rdforCollect2.getResult().get(str) );
            assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkeyForCollect, 0, false).getObject(false).getValue(), rdforCollect2.getResult().get(str));
        }
    }
    
}
