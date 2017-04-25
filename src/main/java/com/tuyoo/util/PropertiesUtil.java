package com.tuyoo.util;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Propertis工具类
 */
public class PropertiesUtil {

    public static Logger LOG = Logger.getLogger(PropertiesUtil.class);

    public static Properties getProperties(String path){
        Properties properties = new Properties();
        try {
            FileInputStream file = new FileInputStream(path);
            properties.load(file);
            LOG.info("文件正常加载："+path);
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return properties;
    }
}
