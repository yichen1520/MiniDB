package com.WangTeng.MiniDB.engine.net.proto.utils;

/**
 * 事务隔离级别定义
 */
public interface Isolations {
    int READ_UNCOMMITTED = 1;
    int READ_COMMITTED = 2;
    int REPEATED_READ = 3;
    int SERIALIZABLE = 4;
}
