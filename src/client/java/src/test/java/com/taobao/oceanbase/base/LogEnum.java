package com.taobao.oceanbase.base;

/**
 * @author baoni
 *
 */
public enum LogEnum {
	MIGRATEDONE("string"),
	MIGRATESTART("long"),
	COPYDONE("time"),
	COPYSTART("float"),
	DAILYMERGESTART("double"),
	DAILYMERGEDONE("precisetime")
	;

    private String type;
    private LogEnum(final String type){
        this.type = type;
    }
 
    public String getType() {
        return type;
    }
    
}
