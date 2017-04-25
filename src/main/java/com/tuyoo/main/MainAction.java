package com.tuyoo.main;

import com.tuyoo.config.BaseConfig;
import com.tuyoo.entities.Hql;
import com.tuyoo.executor.Executor;
import com.tuyoo.executor.OutExecutor;
import com.tuyoo.source.HqlSource;
import com.tuyoo.util.DateUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主程序
 */
public class MainAction {

    private static Logger LOG = Logger.getLogger(MainAction.class);

    public static void main(String[] args) {
        if(args.length == 5){
	    	String propertiesHome = args[0];
    		String maxThreads = args[1];
            final String tableName = args[2];
            final String day = args[3];
            String ids = args[4];
//            String maxThreads = "1";
//            String tableName = "hive_sql";
//            final String day = "2017-04-13";
//            String ids = "-1";
//	    	String propertiesHome = "E:\\mysite\\ideaProjects\\hql-exe\\src\\main\\resources";
            BaseConfig.propertiesHome = propertiesHome.endsWith("/") ? propertiesHome : propertiesHome+"/";
            PropertyConfigurator.configure(BaseConfig.propertiesHome+"log4j.properties");
            ExecutorService sourceService = Executors.newFixedThreadPool(Integer.parseInt(maxThreads));
            final ExecutorService outExecutorService = Executors.newFixedThreadPool(Integer.parseInt(maxThreads)*2);
            HqlSource hqlSource = new HqlSource();
            List<Hql> hqls = hqlSource.getAllHqls(tableName, day, ids);
            int hqlCounts = 0;
            final AtomicInteger executedSql = new AtomicInteger(0);

            //输出需要执行的任务数；
            for(Hql hql:hqls){
                switch (hql.getLogicType()){
                    case "intervals":
                        hqlCounts+= BaseConfig.remain_intervals.length;
                        break;
                    default:
                        hqlCounts+= 1;
                        break;
                }
            }
            LOG.info("执行任务数： " + hqlCounts);

            if(hqlCounts > 0){
//            	ConnectionUtils.invalidateMetadata();
                final OutExecutor out = new OutExecutor();
                final int allCounts = hqlCounts;
                for(final Hql hql:hqls){
                    for (final Entry<Integer, String> entry: hql.parseHql().entrySet()) {
                        sourceService.execute(new Runnable() {
                            @Override
                            public void run() {
                                String sql = entry.getValue();
                                String engine = hql.getEngine();
                                final String logicType = hql.getLogicType();
                                final String sinkType = hql.getSinkType();
                                final String type = hql.getType();
                                final String resultTable = hql.getResultTable();
                                final String exeDay = DateUtils.getPreviousDate(day, entry.getKey());
                                final String sqlId = hql.getId();
                                final String intervals = entry.getKey().toString();
                                final Executor executor = new Executor(sql, engine);
                                final ResultSet resultSet = executor.executeQuery();
                                outExecutorService.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        out.write(resultSet, sinkType, resultTable, logicType, type, exeDay, sqlId, intervals);
                                        executor.close();
                                        LOG.info("共需执行 "+allCounts+" 个，已完成 "+executedSql.addAndGet(1)+" 个");
                                    }
                                });
                            }
                        });
                    }
                }
                sourceService.shutdown();
                try {
                    sourceService.awaitTermination(3, TimeUnit.HOURS);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                } finally {
                    outExecutorService.shutdown();
                }
            }
        } else {
        	System.out.println("args -> <propertiesHome> <maxThreads> <tableName> <day> <ids>");
        }
    }
}
