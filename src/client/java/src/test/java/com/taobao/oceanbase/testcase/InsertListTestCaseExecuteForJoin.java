package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
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

public class InsertListTestCaseExecuteForJoin extends BaseCase {

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

    public boolean getVerify(List<String>listTableName, List<RKey>listRowkey, List<Set<String>>listColSet)
    {
    	boolean bRet = false;
    	Result<RowData> rs = null;
    	boolean iCollectFlag = false;
    	boolean iItemFlag = false;
    	
    	if (listTableName.size() != listRowkey.size() || listRowkey.size() != listColSet.size())
    	{
    		log.error("List size error!!!");
    		return bRet;
    	}
    	
    	for (int iLoop = 0; iLoop < listTableName.size(); iLoop ++)
    	{
    		/* Reset */
    		rs = null;
    		
    		log.info("Get verify table name is " + listTableName.get(iLoop) + ".");
    		log.info("Get verify rowkey is " + listRowkey.get(iLoop).toString() + ".");
    		for(String strTemp : listColSet.get(iLoop))
    		{
    			log.info("Col : " + strTemp);
    		}
    		
    		rs = obClient.get(listTableName.get(iLoop), listRowkey.get(iLoop),listColSet.get(iLoop));
    		if (rs.getCode()!= ResultCode.OB_SUCCESS)
    		{
    			log.error("Rowkey(" + listRowkey.get(iLoop).toString() + ") is failed to get in table(" + listTableName.get(iLoop) + ")!!!");
    			return bRet;
    		}	
    		
            for (String str : listColSet.get(iLoop)) {
            	if (listTableName.get(iLoop).equals("collect_info"))
            	{
            		if (iCollectFlag)
            		{
            			if (rs.getResult() == null)
            			{
            				continue;
            			} else {
                			log.error("Rs which is get from ob is not null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
                			return bRet;
            			}
            		} else {
	            		if (rs.getResult() == null)
	            		{
	            			log.error("Rs which is get from ob is null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		}
	            		if (!collectInfoTable.genColumnUpdateResult(str, listRowkey.get(iLoop), 0, false).getObject(false).getValue().equals(rs.getResult().get(str)))
	            		{
	            			log.error("Col(" + str + ") is failed to verify at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		} else {
	            			log.info("Col(" + str + ") is verified successful at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            		}
            		}
            	} else {
            		if (iItemFlag)
            		{
                		if (rs.getResult() == null)
                		{
                			continue;
                		} else {
                			log.error("Rs which is get from ob is not null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
                			return bRet;
                		}
            		} else {
            			
	            		if (rs.getResult() == null)
	            		{
	            			log.error("Rs which is get from ob is null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		}
	            		
	            		if (!itemInfoTable.genColumnUpdateResult(str, listRowkey.get(iLoop), 0, false).getObject(false).getValue().equals(rs.getResult().get(str)))
	            		{
	            			log.error("Col(" + str + ") is failed to verify at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		} else {
	            			log.info("Col(" + str + ") is verified successful at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            		}
            		}
            	}
            }
            
            if (listTableName.get(iLoop).equals("collect_info"))
            {
            	iCollectFlag = false;
            } else {
            	iItemFlag = false;
            }
            
    	}	/* for */
    	
    	return true;
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
    public void test_insert_list_01_HappyPath() {
    	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	
    	/* Rowkey */
    	/* CollectInfo */
        RKey rowkeyForCollect 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 44));
        RKey rowkeyForCollect2 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 45));
        /* ItemInfo */
        RKey rowkeyForItem 		= new RKey(new ItemInfoKeyRule((byte) '1', 44));
        RKey rowkeyForItem2 	= new RKey(new ItemInfoKeyRule((byte) '1', 45));
        RKey rowkeyForItem3 	= new RKey(new ItemInfoKeyRule((byte) '1', 46));
        
        /* The set of nonjoin cols */
        /* CollectInfo */
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        Set<String> ColumnsforCollectInsert2 = setInsertColumnsforCollect();
        /* ItemInfo */
        InsertMutator imforItem 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem ,itemInfoTable.getAllColumnNames(), 0);
        InsertMutator imforItem2 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem2,itemInfoTable.getAllColumnNames(), 0);
        InsertMutator imforItem3 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem3,itemInfoTable.getAllColumnNames(), 0);      
        /* Set mutator */
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,ColumnsforCollectInsert , 0);
        InsertMutator imforCollect2 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect2,ColumnsforCollectInsert2, 0);
        /* Set list */
        listMutator.add(imforCollect1);
        listMutator.add(imforCollect2);
        listMutator.add(imforItem);
        listMutator.add(imforItem2);
        listMutator.add(imforItem3);
        
        listTableName.add(collectInfoTable.getTableName());
        listTableName.add(collectInfoTable.getTableName()); 
        listTableName.add(itemInfoTable.getTableName());
        listTableName.add(itemInfoTable.getTableName());
        listTableName.add(itemInfoTable.getTableName());
        
        listRowkey.add(rowkeyForCollect);
        listRowkey.add(rowkeyForCollect2);
        listRowkey.add(rowkeyForItem);
        listRowkey.add(rowkeyForItem2);
        listRowkey.add(rowkeyForItem3);
        
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(itemInfoTable.getAllColumnNames());
        listColSet.add(itemInfoTable.getAllColumnNames());
        listColSet.add(itemInfoTable.getAllColumnNames());
        
        /* Insert */
        Result<Boolean> rs = obClient.insert(listMutator);
        assertTrue("Insert fail!", rs.getResult());
        
        /* For test */
//        Result<RowData> rsForTest = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, ColumnsforCollectInsert);
//        assertTrue(rsForTest.getResult()==null);
        
        /* Verify */
        Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet));
    }
    
    @Test
    public void test_insert_list_02_null() {
    	/* List */
    	List<InsertMutator> listMutator = null;
        
        /* Insert */
        Result<Boolean> rs = null;
        try{
        	rs = obClient.insert(listMutator);
        	Assert.fail("insert a null mutator");
        } catch (IllegalArgumentException e){
        	assertTrue(e.getMessage(), rs==null);
        }
    }
    
    @Test
    public void test_insert_list_03_empty() {
    	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
        
        /* Insert */
        Result<Boolean> rs = null;
        try{
        	rs = obClient.insert(listMutator);
        	Assert.fail("insert null");
        } catch (IllegalArgumentException e){
        	Assert.assertTrue(e.getMessage(),rs==null);
        }
    }
    
    @Test
    public void test_insert_list_04_one_null() {
       	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	
    	/* Rowkey */
    	/* CollectInfo */
        RKey rowkeyForCollect 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 44));
        
        /* ItemInfo */
//        RKey rowkeyForItem 		= new RKey(new ItemInfoKeyRule((byte) '1', 44));
        
        /* The set of nonjoin cols */
        /* CollectInfo */
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();

        /* ItemInfo */
//        InsertMutator imforItem 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem ,itemInfoTable.getAllColumnNames(), 0);
        
        /* Set mutator */
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,ColumnsforCollectInsert , 0);

        /* Set list */
        listMutator.add(imforCollect1);
        listMutator.add(null);
        
        listTableName.add(collectInfoTable.getTableName());
        listTableName.add(null);
        
        listRowkey.add(rowkeyForCollect);
        listRowkey.add(null);
        
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(null);
        
        /* Insert */
        Result<Boolean> rs = null;
        try{
        	rs = obClient.insert(listMutator);
        	Assert.fail("insert a null");
        } catch (IllegalArgumentException e){
        	assertTrue(e.getMessage(), rs==null);
        }
        
        
        /* Verify */
        //Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet)); 
    }
    
    @Test
    public void test_insert_list_05_onerow() {
    	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	
    	/* Rowkey */
    	/* CollectInfo */
        RKey rowkeyForCollect 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 44));
        /* ItemInfo */
        RKey rowkeyForItem 		= new RKey(new ItemInfoKeyRule((byte) '1', 44));
   
        /* The set of nonjoin cols */
        /* CollectInfo */
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();

        /* ItemInfo */
        InsertMutator imforItem 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem ,itemInfoTable.getAllColumnNames(), 0);
     
        /* Set mutator */
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,ColumnsforCollectInsert , 0);

        /* Set list */
        listMutator.add(imforCollect1);
        listMutator.add(imforItem);
        
        listTableName.add(collectInfoTable.getTableName()); 
        listTableName.add(itemInfoTable.getTableName());
        
        listRowkey.add(rowkeyForCollect);
        listRowkey.add(rowkeyForItem);
        
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(itemInfoTable.getAllColumnNames());

        
        /* Insert */
        Result<Boolean> rs = obClient.insert(listMutator);
        assertTrue("Insert fail!", rs.getResult());
        
        /* Verify */
        Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet));
    }
    
    @Test
    public void test_insert_list_06_diffrow() {
    	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	
    	/* Rowkey */
    	/* CollectInfo */
        RKey rowkeyForCollect 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 44));
        /* ItemInfo */
        RKey rowkeyForItem 		= new RKey(new ItemInfoKeyRule((byte) '1', 45));
   
        /* The set of nonjoin cols */
        /* CollectInfo */
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();

        /* ItemInfo */
        InsertMutator imforItem 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem ,itemInfoTable.getAllColumnNames(), 0);
     
        /* Set mutator */
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,ColumnsforCollectInsert , 0);

        /* Set list */
        listMutator.add(imforCollect1);
        listMutator.add(imforItem);
        
        listTableName.add(collectInfoTable.getTableName()); 
        listTableName.add(itemInfoTable.getTableName());
        
        listRowkey.add(rowkeyForCollect);
        listRowkey.add(rowkeyForItem);
        
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(itemInfoTable.getAllColumnNames());

        
        /* Insert */
        Result<Boolean> rs = obClient.insert(listMutator);
        assertTrue("Insert fail!", rs.getResult());
        
        /* Verify */
        Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet));
    }
    
    @Test
    public void test_insert_list_07_one_table_onerow() {
    	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	
    	/* Rowkey */
    	/* CollectInfo */
        RKey rowkeyForCollect 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 44));
   
        /* The set of nonjoin cols */
        /* CollectInfo */
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
     
        /* Set mutator */
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,ColumnsforCollectInsert , 0);

        /* Set list */
        listMutator.add(imforCollect1);
        
        listTableName.add(collectInfoTable.getTableName()); 
        
        listRowkey.add(rowkeyForCollect);
        
        listColSet.add(ColumnsforCollectInsert);

        
        /* Insert */
        Result<Boolean> rs = obClient.insert(listMutator);
        assertTrue("Insert fail!", rs.getResult());
        
        /* Verify */
        Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet));
    }
    
    @Test
    public void test_insert_list_08_one_error() {
    	/* List */
    	List<InsertMutator> listMutator = new ArrayList<InsertMutator>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	
    	/* Rowkey */
    	/* CollectInfo */
        RKey rowkeyForCollect 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 44));
        RKey rowkeyForCollect2 	= new RKey(new CollectInfoKeyRule(3, (byte) '1', 45));
        /* ItemInfo */
        RKey rowkeyForItem 		= new RKey(new ItemInfoKeyRule((byte) '1', 44));
        RKey rowkeyForItem2 	= new RKey(new ItemInfoKeyRule((byte) '1', 45));
        RKey rowkeyForItem3 	= new RKey(new ItemInfoKeyRule((byte) '1', 46));
        
        /* The set of nonjoin cols */
        /* CollectInfo */
        Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
        Set<String> ColumnsforCollectInsert2 = setInsertColumnsforCollect();
        /* ItemInfo */
        InsertMutator imforItem 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem ,itemInfoTable.getAllColumnNames(), 0);
        InsertMutator imforItem2 	= getNormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem2,itemInfoTable.getAllColumnNames(), 0);
        InsertMutator imforItem3 	= getNormalIMforItem(collectInfoTable.getTableName(), rowkeyForItem3,itemInfoTable.getAllColumnNames(), 0);      
        /* Set mutator */
        InsertMutator imforCollect1 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect,ColumnsforCollectInsert , 0);
        InsertMutator imforCollect2 = getNormalIMforCollect(collectInfoTable.getTableName(), rowkeyForCollect2,ColumnsforCollectInsert2, 0);
        /* Set list */
        listMutator.add(imforCollect1);
        listMutator.add(imforCollect2);
        listMutator.add(imforItem);
        listMutator.add(imforItem2);
        listMutator.add(imforItem3);
        
        listTableName.add(collectInfoTable.getTableName());
        listTableName.add(collectInfoTable.getTableName()); 
        listTableName.add(itemInfoTable.getTableName());
        listTableName.add(itemInfoTable.getTableName());
        listTableName.add(itemInfoTable.getTableName());
        
        listRowkey.add(rowkeyForCollect);
        listRowkey.add(rowkeyForCollect2);
        listRowkey.add(rowkeyForItem);
        listRowkey.add(rowkeyForItem2);
        listRowkey.add(rowkeyForItem3);
        
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(ColumnsforCollectInsert);
        listColSet.add(itemInfoTable.getAllColumnNames());
        listColSet.add(itemInfoTable.getAllColumnNames());
        listColSet.add(itemInfoTable.getAllColumnNames());
        
        /* Insert */
        Result<Boolean> rs = obClient.insert(listMutator);
        assertFalse("Insert fail!", rs.getResult());
        
        /* For test */
//        Result<RowData> rsForTest = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, ColumnsforCollectInsert);
//        assertTrue(rsForTest.getResult()==null);
        
        listTableName.clear();
        listRowkey.clear();
        listColSet.clear();
        
        listTableName.add(collectInfoTable.getTableName());
        listRowkey.add(rowkeyForCollect);
        listColSet.add(ColumnsforCollectInsert);
        
        /* Verify */
        Assert.assertFalse(getVerify(listTableName, listRowkey, listColSet));
    }
    
}
