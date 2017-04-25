package com.tuyoo.executor;

import com.tuyoo.sink.*;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 输出
 */
public class OutExecutor {

    private Logger LOG = Logger.getLogger(OutExecutor.class);

    private Map<String, Lock> locks = new ConcurrentHashMap<>();

    private synchronized Lock getLock(String resultTable){
        if(!locks.containsKey(resultTable)){
            locks.put(resultTable, new ReentrantLock());
        }
        return this.locks.get(resultTable);
    }

    public void write(ResultSet resultSet, String sinkType, String resultTable, String logicType, String type, String exeDay, String sqlId, String intervals){
        Lock lock = getLock(resultTable);
        lock.lock();
        LOG.info("执行写入。。。"+ sqlId+"\t"+resultTable);
        MysqlSink sink = null;
        switch (sinkType) {
            case "json":
                sink = new JsonMysqlSink(resultTable, logicType, type, exeDay, sqlId, intervals);
                break;
            case "base":
                sink = new BaseMysqlSink(resultTable, logicType, type, exeDay, sqlId, intervals);
                break;
            case "update":
                sink = new UpdateMysqlSink(resultTable, logicType, type, exeDay, sqlId, intervals);
                break;
            case "old":
                sink = new OldMysqlSink(resultTable, logicType, type, exeDay, sqlId, intervals);
            default:
                break;
        }
        if(sink != null){
            sink.setResultSet(resultSet);
            sink.write();
        } else {
            LOG.error("无效的sink 类型。。" + sqlId);
        }
        LOG.info("写入完毕。。。"+ sqlId+"\t"+resultTable);
        lock.unlock();
    }
}
