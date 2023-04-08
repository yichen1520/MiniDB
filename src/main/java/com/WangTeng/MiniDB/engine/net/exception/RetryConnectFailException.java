package com.WangTeng.MiniDB.engine.net.exception;


public class RetryConnectFailException extends RuntimeException {

    public RetryConnectFailException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryConnectFailException(String message) {
        super(message);
    }
}
