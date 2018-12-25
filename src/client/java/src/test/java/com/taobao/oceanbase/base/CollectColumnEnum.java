package com.taobao.oceanbase.base;

/**
 * @author baoni
 *
 */
public enum CollectColumnEnum {
	USER_NICK("user_nick","string"),
	IS_SHARED("is_shared","long"),
	NOTE("note","string"),
	COLLECT_TIME("collect_time","time"),
	STATUS("status","long"),
	GM_CREATE("gm_create","time"),
	GM_MODIFIED("gm_modified","time"),
	TAG("tag","string"),
	CATEGORY("category","long"),
	TITLE("title","string"),
	PICURL("picurl","string"),
	OWNER_ID("owner_id","long"),
	OWNER_NICK("owner_nick","string"),
	PRICE("price","float"),
	ENDS("ends","time"),
	PROPER_XML("proper_xml","string"),
	COMMENT_COUNT("comment_count","long"),
	COLLECTOR_COUNT("collector_count","long"),
	COLLECT_COUNT("collect_count","long"),
	TOTAL_MONEY("total_money","double"),
	MONEY("money","double"),
	COLLECT_TIME_PRECISE("collect_time_precise","precisetime")
	;

    private String value;
    private String type;
    private CollectColumnEnum(final String value,final String type){
        this.value = value;
        this.type = type;
    }
    public String getValue() {
        return value;
    }
    public String getType() {
        return type;
    }
    
}
