package com.taobao.oceanbase.base;

/**
 * @author baoni
 *
 */
public enum ColumnTypeEnum {
	STRING("string"),
	LONG("long"),
	TIME("time"),
	FLOAT("float"),
	DOUBLE("double"),
	PRECISETIME("precisetime")
	;

    private String type;
    private ColumnTypeEnum(final String type){
        this.type = type;
    }
 
    public String getType() {
        return type;
    }
    
}
