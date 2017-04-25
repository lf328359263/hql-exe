package com.tuyoo.util;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 日期工具类
 */
public class DateUtils {

    public static Logger LOG = Logger.getLogger(ConnectionUtils.class);
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public synchronized static String getPreviousDate(String day, int interval){
        return getNextDate(day, -interval);
    }

    public static String getNextDate(String day, int interval){
        Calendar instance = Calendar.getInstance();
        try {
            instance.setTime(simpleDateFormat.parse(day));
            instance.add(Calendar.DATE, interval);
            return simpleDateFormat.format(instance.getTime());
        } catch (ParseException e) {
            LOG.error(e.getMessage());
            return "";
        }
    }
}
