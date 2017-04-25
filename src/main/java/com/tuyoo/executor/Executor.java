package com.tuyoo.executor;

import com.tuyoo.util.ConnectionUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Executor {

    public Logger LOG = Logger.getLogger(Executor.class);

    private String hql;
    private String engine;
    private Connection connection;

    public Executor(String hql, String engine) {
        super();
        this.hql = hql;
        this.engine = engine;
    }

    public ResultSet executeQuery() {
        ResultSet resultSet = null;
        switch (this.engine) {
            case "mysql":
                LOG.info("mysql执行： " + this.hql);
                connection = ConnectionUtils.getMysqlConnction();
                break;
            default:
                LOG.info("impala执行： " + this.hql);
                connection = ConnectionUtils.getImpalaConn();
                break;
        }
        try {
            resultSet = connection.prepareStatement(this.hql).executeQuery();
            LOG.info("sql执行完毕。。。" + this.hql);
        } catch (SQLException e) {
            LOG.error(this.hql+"\n"+e.getMessage());
        }
        return resultSet;
    }

    public void close(){
        if(this.connection != null){
            try {
                connection.close();
                LOG.info("本次impala连接关闭。。。");
            } catch (SQLException e) {
                LOG.error(e.getMessage());
            }
        }
    }
}
