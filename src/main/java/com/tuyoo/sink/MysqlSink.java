package com.tuyoo.sink;

import java.sql.ResultSet;

/**
 * 写数据至mysql
 */
public abstract class MysqlSink {
	
	protected ResultSet resultSet;
	protected String tableName;
	protected String logicType;
	protected String type;
	protected String day;
	protected String sqlId;
	protected String intervals;
	
    public abstract void write();

    public void setResultSet(ResultSet resultSet){
    	this.resultSet = resultSet;
	}

}
