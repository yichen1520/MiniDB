package com.WangTeng.MiniDB.config;

import com.WangTeng.MiniDB.engine.net.proto.utils.Isolations;

public interface SystemConfig {
    int DEFAULT_PAGE_SIZE = 4096;   //4KB

    int DEFAULT_SPECIAL_POINT_LENGTH = 64;

    String RELATION_FILE_PRE_FIX = "D:/MiniDB/";

    String MINIDB_REL_DATA_PATH = RELATION_FILE_PRE_FIX + "/data";

    String MINIDB_REL_META_PATH = RELATION_FILE_PRE_FIX + "/meta";

    String MINIDB_LOG_FILE_NAME = RELATION_FILE_PRE_FIX + "/log/log";

    String Database = "";

    // 36小时内连接不发起请求就干掉 秒为单位
    long IDLE_TIME_OUT = 36 * 3600;

    // 1小时做一次idle check 秒为单位
    int IDLE_CHECK_INTERVAL = 3600;

    String DEFAULT_CHARSET = "gbk";

    int DEFAULT_TX_ISOLATION = Isolations.REPEATED_READ;
}
