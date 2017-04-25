package com.tuyoo.entities;

import com.tuyoo.config.BaseConfig;
import com.tuyoo.util.DateUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 将数据库中的HQL语句解析后，转换为类对象
 */
public class Hql {
    private String id;
    private String hql;
    private String type;
    private String resultTable;
    private String isExe;
    private String instruction;
    private String day;
    private String engine;
    private String logicType;
    private String sinkType;
    
    public String getEngine() {
		return engine;
	}

	public void setEngine(String engine) {
		this.engine = engine;
	}

	public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getLogicType() {
        return logicType;
    }

    public void setLogicType(String logicType) {
        this.logicType = logicType;
    }

    public String getHql() {
        return hql;
    }

    public void setHql(String hql) {
        this.hql = hql;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getResultTable() {
        return resultTable;
    }

    public void setResultTable(String resultTable) {
        this.resultTable = resultTable;
    }

    public String getIsExe() {
        return isExe;
    }

    public void setIsExe(String exe) {
        isExe = exe;
    }

    public String getInstruction() {
        return instruction;
    }

    public void setInstruction(String instruction) {
        this.instruction = instruction;
    }

    public String getId() {

        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSinkType() {
		return sinkType;
	}

	public void setSinkType(String sinkType) {
		this.sinkType = sinkType;
	}

	public Map<Integer,String> parseHql(){
        Map<Integer,String> hqls = new HashMap<Integer,String>();
        switch (this.logicType) {
            case "intervals":
                for(Integer i : BaseConfig.remain_intervals){
                    String previousDay = DateUtils.getPreviousDate(this.day, i);
                    hqls.put(i, this.hql.replaceAll("\\$pre_day",previousDay).replaceAll("\\$day",this.day));
                }
                break;
            default:
                hqls.put(0, this.hql.replaceAll("\\?","'"+this.day+"'"));
                break;
        }
        return hqls;
    }

    public Hql(String id, String hql, String type, String resultTable, String isExe, String instruction, String day, String sinkType, String logicType, String engine) {
        this.id = id;
        this.hql = hql;
        this.type = type;
        this.resultTable = resultTable;
        this.isExe = isExe;
        this.instruction = instruction;
        this.day = day;
        this.sinkType = sinkType;
        this.logicType = logicType;
        this.engine = engine;
    }
}
