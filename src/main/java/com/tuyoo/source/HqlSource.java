package com.tuyoo.source;

import com.tuyoo.entities.Hql;
import com.tuyoo.util.ConnectionUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 从mysql加载HQL对象
 */
public class HqlSource {

    private Logger LOG = Logger.getLogger(HqlSource.class);

    /**
     *
     * @param tableName
     * @param day
     */
    public List<Hql> getAllHqls(String tableName, String day, String ids){
        Connection connection = ConnectionUtils.getMysqlConnction();
        List<Hql> hqls = new ArrayList<Hql>();
        String sql = "SELECT * FROM " + tableName;
        switch(ids){
        	case "-1": 
        		sql+=" WHERE is_exe = 1";
        		break;
        	default:
        		sql+=" WHERE id IN ("+ids+")";
        }
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()){
                Hql hql = new Hql(
                        resultSet.getString("id"),
                        resultSet.getString("exe_sql"),
                        resultSet.getString("type"),
                        resultSet.getString("table_name"),
                        resultSet.getString("is_exe"),
                        resultSet.getString("instruction"),
                        day,
                        resultSet.getString("sink_type"),
                        resultSet.getString("logic_type"),
                        resultSet.getString("engine")
                        );
                hqls.add(hql);
            }
            statement.close();
            connection.close();
            LOG.info("本次执行sql数："+hqls.size());
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }
        return hqls;
    }
}
