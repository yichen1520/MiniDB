package com.WangTeng.MiniDB.engine.parser.util;

public class CharTypes {
    private final static boolean[] hexFlags = new boolean[256];

    //将对应的十六进制字符设为true
    static {
        for (char c = 0; c < hexFlags.length; ++c) {
            if (c >= 'A' && c <= 'F') {
                hexFlags[c] = true;
            } else if (c >= 'a' && c <= 'f') {
                hexFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                hexFlags[c] = true;
            }
        }
    }

    /**
     * 判断字符是否为十六进制时，需要先判断输入字符是否小于 256，以确保只考虑 Unicode 或 ANSI 字符集中的字符
     */
    public static boolean isHex(char c) {
        return c < 256 && hexFlags[c];
    }

    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    //标识符标志
    private static final boolean[] identifierFlags = new boolean[256];

    static {
        for (char c = 0; c < identifierFlags.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                identifierFlags[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                identifierFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                identifierFlags[c] = true;
            }
        }
        identifierFlags['_'] = true;
        identifierFlags['$'] = true;
    }

    /**
     * Unicode编码中字符大于256的仍然可以用作标识符
     */
    public static boolean isIdentifierChar(char c) {
        return c > identifierFlags.length || identifierFlags[c];
    }

    private final static boolean[] whitespaceFlags = new boolean[256];

    static {
        whitespaceFlags[' '] = true;
        whitespaceFlags['\n'] = true;   //换行
        whitespaceFlags['\r'] = true;   //回车
        whitespaceFlags['\t'] = true;   //制表符
        whitespaceFlags['\f'] = true;   //换页符
        whitespaceFlags['\b'] = true;   //退格
    }

    public static boolean isWhitespace(char c) {
        return c <= whitespaceFlags.length && whitespaceFlags[c];
    }
}
