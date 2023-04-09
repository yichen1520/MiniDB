package com.WangTeng.MiniDB.transaction.log;

public interface LogType {

    int TRX_START = 0;

    int ROLL_BACK = 1;

    int COMMIT = 2;

    int ROW = 3;
}
