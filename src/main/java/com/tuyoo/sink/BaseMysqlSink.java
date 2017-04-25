package com.tuyoo.sink;

import com.tuyoo.util.ConnectionUtils;

import java.sql.*;

import org.apache.log4j.Logger;

/**
 * 标准数据写入  replace（不包含sql_id）
 */
public class BaseMysqlSink extends MysqlSink {
	
	private Logger LOG = Logger.getLogger(BaseMysqlSink.class);
	
    public BaseMysqlSink(String tableName,
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
    public void write() {
        try {
            Connection connection = ConnectionUtils.getMysqlConnction();
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement(insertSql());
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while(resultSet.next()){
                for(int i = 0; i<columnCount; i++){
                    statement.setString(i+1, resultSet.getString(i+1));
                }
                switch (logicType){
                    case "intervals":
                        statement.setString(columnCount+1, type);
                        statement.setString(columnCount+2, day);
                        statement.setString(columnCount+3, intervals);
                        break;
                    default:
                        statement.setString(columnCount+1, type);
                        statement.setString(columnCount+2, day);
                        break;
                }
                statement.addBatch();
            }
            LOG.info("准备批量写入："+sqlId);
            statement.executeBatch();
            statement.close();
            connection.close();
            LOG.info("批量写入完毕："+sqlId);
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }
    }

    public String insertSql() {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            StringBuffer sb = new StringBuffer("replace into ");
            StringBuffer suffix = new StringBuffer(") values (");
            sb.append(tableName);
            sb.append(" (");
            for (int i = 0; i < columnCount; i++) {
                if(i!=0){
                    sb.append(",");
                    suffix.append(",");
                }
                sb.append(metaData.getColumnLabel(i+1));
                suffix.append("?");
            }
            switch (logicType) {
                case "intervals":
                    sb.append(",type,day,intervals");
                    suffix.append(",?,?,?");
                    break;
                default:
                    sb.append(",type,day");
                    suffix.append(",?,?");
                    break;
            }
            String result = sb.append(suffix).append(")").toString();
            LOG.info("sql:"+result);
            return result;
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            return null;
        }
    }
}
