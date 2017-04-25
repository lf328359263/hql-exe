package com.tuyoo.sink;

import com.tuyoo.config.BaseConfig;
import com.tuyoo.util.ConnectionUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * 更新表数据
 */
public class UpdateMysqlSink extends MysqlSink{

    private Logger LOG = Logger.getLogger(UpdateMysqlSink.class);

    private String updateSql ;
    private String insertSql ;
    private List<String> selectFields = Arrays.asList(BaseConfig.indexFields);
    private String valueName;
    private ResultSetMetaData metaData;

    public UpdateMysqlSink(String tableName,
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
        this.setValueName();
        this.setInsertSql();
        this.setUpdateSql();
    }

    private void setValueName(){
        if(this.valueName == null){
            try {
                int columnCount = this.metaData.getColumnCount();
                for(int i = 0; i<columnCount; i++){
                    if(!selectFields.contains(metaData.getColumnLabel(i+1))){
                        this.valueName = metaData.getColumnLabel(i+1);
                        break;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void setUpdateSql(){
        if(this.updateSql == null){
            StringBuffer preSql = new StringBuffer("update ");
            preSql.append(tableName);
            preSql.append(" set ");
            preSql.append(valueName);
            preSql.append(" = '$");
            preSql.append(valueName);
            preSql.append("'");
            StringBuffer sufSql = new StringBuffer(" where day = '$day'");
            try {
                int count = this.metaData.getColumnCount();
                for(int i = 0; i< count; i++){
                    String label = this.metaData.getColumnLabel(i+1);
                    if(!label.equals(this.valueName)){
                        sufSql.append(" and ");
                        sufSql.append(label).append(" = '$").append(label).append("'");
                    }
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
            StringBuffer preSql = new StringBuffer("insert into "+tableName+" ("+valueName);
            StringBuffer sufSql = new StringBuffer(") values ('$"+this.valueName+"'");
            try {
                int count = this.metaData.getColumnCount();
                for(int i = 0; i< count; i++){
                    String label = this.metaData.getColumnLabel(i+1);
                    if(!label.equals(this.valueName)){
                        preSql.append(", ");
                        sufSql.append(", ");
                        preSql.append(label);
                        sufSql.append("'$").append(label).append("'");
                    }
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
                sql = sql.replace("$day", this.day);
                int update = statement.executeUpdate(sql);
                if(update < 1){
                    sql = this.insertSql;
                    for(int i = 0; i < count; i++){
                        sql = sql.replace("$"+this.metaData.getColumnLabel(i+1), resultSet.getString(i+1));
                    }
                    sql = sql.replace("$day", this.day);
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
}
