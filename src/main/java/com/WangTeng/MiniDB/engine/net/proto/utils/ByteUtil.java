package com.WangTeng.MiniDB.engine.net.proto.utils;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import io.netty.buffer.ByteBuf;

/**
 * 读取字节以及解码，小序列方式 后读的放在前面存储
 */
public class ByteUtil {
    public static int readUB2(ByteBuf data) {
        int i = data.readByte() & 0xff;
        i |= (data.readByte() & 0xff) << 8;
        return i;
    }

    public static int readUB3(ByteBuf data) {
        int i = data.readByte() & 0xff;
        i |= (data.readByte() & 0xff) << 8;
        i |= (data.readByte() & 0xff) << 16;
        return i;
    }

    public static long readUB4(ByteBuf data) {
        long l = data.readByte() & 0xff;
        l |= (data.readByte() & 0xff) << 8;
        l |= (data.readByte() & 0xff) << 16;
        l |= (data.readByte() & 0xff) << 24;
        return l;
    }

    public static long readLong(ByteBuf data) {
        long l = (long) (data.readByte() & 0xff);
        l |= (long) (data.readByte() & 0xff) << 8;
        l |= (long) (data.readByte() & 0xff) << 16;
        l |= (long) (data.readByte() & 0xff) << 24;
        l |= (long) (data.readByte() & 0xff) << 32;
        l |= (long) (data.readByte() & 0xff) << 40;
        l |= (long) (data.readByte() & 0xff) << 48;
        l |= (long) (data.readByte() & 0xff) << 56;
        return l;
    }

    /**
     * 二进制数据读取长度大小时，先判断第一个字节的数值大小
     * @param data
     * @return 当前字节数据的长度大小
     */
    public static long readLength(ByteBuf data) {
        int length = data.readByte() & 0xff;
        switch (length) {
            case 251:
                return MySQLMessage.NULL_LENGTH;
            case 252:
                return readUB2(data);
            case 253:
                return readUB3(data);
            case 254:
                return readLong(data);
            default:
                return length;
        }
    }

    /**
     * @param src
     * @return 解析拿到当前字节数组形成的数据包长度(src + 定义的长度占用大小)
     */
    public static int decodeLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static int decodeLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

}
