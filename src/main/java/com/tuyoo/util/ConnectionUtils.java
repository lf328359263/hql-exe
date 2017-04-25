package com.tuyoo.util;

import com.tuyoo.config.BaseConfig;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 连接工具类
 */
public class ConnectionUtils {

    public static Logger LOG = Logger.getLogger(ConnectionUtils.class);

    public static Connection getMysqlConnction(){
        Properties properties = PropertiesUtil.getProperties(BaseConfig.propertiesHome+ BaseConfig.mysqlPropertiesName);
        try {
            LOG.info("获取mysql连接。。。");
            Class.forName(properties.getProperty("driver"));
            return DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("user"), properties.getProperty("password"));
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            return null;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            return null;
        }
    }

    public static Connection getImpalaConn() {
        Properties properties = PropertiesUtil.getProperties(BaseConfig.propertiesHome+ BaseConfig.impalaPropertiesName);
        try {
            LOG.info("获取impala连接。。。");
            Class.forName(properties.getProperty("driver"));
            return DriverManager.getConnection(properties.getProperty("url"));
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            return null;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            return null;
        }
    }

    public static boolean invalidateMetadata(){
    	LOG.info("全局元数据刷新");
    	Connection connection = getImpalaConn();
    	boolean flag = false;
    	try {
            connection.createStatement().execute("invalidate metadata");
            LOG.info("元数据刷新成功");
            flag = true;
    	} catch (SQLException e) {
        	LOG.error("元数据刷新失败");
            LOG.error(e.getMessage());
        } finally {
        	try {
				connection.close();
				LOG.info("连接关闭");
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
        }
    	return flag;
    }
}
