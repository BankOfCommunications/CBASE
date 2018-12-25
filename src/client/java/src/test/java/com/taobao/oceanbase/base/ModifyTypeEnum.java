package com.taobao.oceanbase.base;

/**
 * @author baoni
 *
 */
public enum ModifyTypeEnum {
	NULL("NULL"),
	UPDATE("UPDATE"),
	DELETE("DELETE"),
	INSERT("INSERT");

    private String value;
    private ModifyTypeEnum(final String value){
        this.value = value;
    }
    public String getValue() {
        return value;
    }
}
