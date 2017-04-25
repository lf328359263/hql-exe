package com.tuyoo.config;

/**
 * 基本配置
 */
public class BaseConfig {
	
//	public static String propertiesHome = "/home/cdh/count-java/config/";
	public static String propertiesHome ;
//	public static String propertiesHome = "E:\\mysite\\ideaProjects\\impala-count-new\\src\\main\\resources\\";
	public static String mysqlPropertiesName = "mysql.properties";
	public static String impalaPropertiesName = "impala.properties";
	public static Integer[] remain_intervals = {1, 2, 3, 4, 5, 6, 14, 29, 44, 59, 74, 89};
	public static String[] indexFields = {"day","platform_id","product_id","game_id","channel_id","product_nickname","client_id","user_id","room_id"};
}
