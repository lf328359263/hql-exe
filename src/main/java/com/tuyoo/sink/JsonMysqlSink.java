package com.tuyoo.sink;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.tuyoo.util.ConnectionUtils;

public class JsonMysqlSink extends MysqlSink{
	
	private Logger LOG = Logger.getLogger(JsonMysqlSink.class);
	
	private String updateSql ;
	private String insertSql ;
	private String key ="type";
	private String value ="value";
	private ResultSetMetaData metaData;
	
	public JsonMysqlSink(String tableName,
			String logicType, String type, String day, String sqlId,
			String intervals) {
		super();
		this.tableName = tableName;
		this.logicType = logicType;
		this.type = type;
		this.day = day;
		this.sqlId = sqlId;
		this.intervals = intervals;
	}

	@Override
	public void setResultSet(ResultSet resultSet) {
		super.setResultSet(resultSet);
		try {
			this.metaData = resultSet.getMetaData();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		this.setInsertSql();
		this.setUpdateSql();
	}

	@Override
	public void write() {
		Connection connection = ConnectionUtils.getMysqlConnction();
		try {
			Statement statement = connection.createStatement();
			int count = this.metaData.getColumnCount();
			while(this.resultSet.next()){
				String sql = this.updateSql;
				for(int i = 0; i < count; i++){
					sql = sql.replace("$"+this.metaData.getColumnLabel(i+1), resultSet.getString(i+1));
				}
				sql = sql.replace("$day", this.day).replace("$type", this.type).replace("$intervals", this.intervals);
				int update = statement.executeUpdate(sql);
				if(update < 1){
					sql = this.insertSql;
					for(int i = 0; i < count; i++){
						sql = sql.replace("$"+this.metaData.getColumnLabel(i+1), resultSet.getString(i+1));
					}
					sql  = sql.replace("$day", this.day).replace("$type", this.type).replace("$intervals", this.intervals);
					statement.execute(sql);
				}
			}
			statement.close();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
		}
	}
	
	private void setUpdateSql(){
		if(this.updateSql == null){
			StringBuffer preSql = new StringBuffer("update ");
			preSql.append(tableName);
			preSql.append(" set value = JSON_SET(value, '$.\"$"+this.key+"\"', '$"+this.value+"')");
			StringBuffer sufSql = new StringBuffer(" where day = '$day'");
			try {
				int count = this.metaData.getColumnCount();
				for(int i = 0; i< count; i++){
					String label = this.metaData.getColumnLabel(i+1);
					if(!label.equals(this.key) && !label.equals(this.value)){
						sufSql.append(" and ");
						sufSql.append(label).append(" = '$").append(label).append("'");
					}
				}
				switch (this.logicType) {
				case "intervals":
					sufSql.append(" and intervals = '$intervals'");
					break;
				default:
					break;
				}
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
			this.updateSql = preSql.append(sufSql).toString();
			LOG.info("生成更新语句。。。"+this.updateSql);
		}
	}
	
	private void setInsertSql(){
		if(this.insertSql == null){
			StringBuffer preSql = new StringBuffer("insert into "+tableName+" (value, day");
			StringBuffer sufSql = new StringBuffer(") values (JSON_OBJECT('$"+this.key+"', '$"+this.value+"'), '$day'");
			try {
				int count = this.metaData.getColumnCount();
				for(int i = 0; i< count; i++){
					String label = this.metaData.getColumnLabel(i+1);
					if(!label.equals(this.key) && !label.equals(this.value)){
						preSql.append(", ");
						sufSql.append(", ");
						preSql.append(label);
						sufSql.append("'$").append(label).append("'");
					}
				}
				if(!"type".equals(this.key)){
					preSql.append(", type");
					sufSql.append(", '$type'");
				}
				switch (this.logicType) {
				case "intervals":
					preSql.append(", intervals");
					sufSql.append(", '$intervals'");
					break;
				default:
					break;
				}
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
			this.insertSql = preSql.append(sufSql).append(")").toString();
			LOG.info("生成插入语句。。。"+this.insertSql);
		}
	}

	public String getInsertSql() {
		return insertSql;
	}

	public String getUpdateSql() {
		return updateSql;
	}

}
