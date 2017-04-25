package com.tuyoo.executor;

import com.tuyoo.util.ConnectionUtils;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * impala执行 sql
 */
public class ImpalaExecutor {
	
	private Logger LOG = Logger.getLogger(ImpalaExecutor.class);
	
//    private String tableName;
//    private String logicType;
//    private String type;
//    private String day;
//    private String sqlId;
//    private String intervals;
//    private String hql;

//    public ImpalaExecutor(String tableName, String logicType, String type, String day, String sqlId, String intervals, String hql) {
//        this.tableName = tableName;
//        this.logicType = logicType;
//        this.type = type;
//        this.day = day;
//        this.sqlId = sqlId;
//        this.intervals = intervals;
//        this.hql = hql;
//    }

    public void executeQuery() {
        Connection connection = ConnectionUtils.getImpalaConn();
//        try {
//            ResultSet resultSet = connection.prepareStatement(this.hql).executeQuery();
//            MysqlSink sink = new BaseMysqlSink();
//            sink.write(resultSet, this.tableName, this.logicType, this.type, this.day, this.sqlId, this.intervals);
//        } catch (SQLException e) {
//            LOG.error(e.getMessage());
//        } finally {
            try {
                connection.close();
                LOG.info("本次impala连接关闭。。。");
            } catch (SQLException e) {
                LOG.error(e.getMessage());
            }
//        }
    }
}
