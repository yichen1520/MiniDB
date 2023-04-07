package com.WangTeng.MiniDB.engine.net.utils;

import io.netty.buffer.ByteBuf;

public class BufferUtil {

    public static final void writeByte(ByteBuf buffer, byte b) {
        buffer.writeByte(b);
    }

    public static final void writeBytes(ByteBuf buffer, byte[] b) {
        buffer.writeBytes(b);
    }

    //采用小序列的方式写入 强转会丢弃高位，右移读
    public static final void writeUB2(ByteBuf buffer, int i) {  //先写低位，将低位放前面
        buffer.writeByte((byte) (i & 0xff));
        buffer.writeByte((byte) (i >>> 8));
    }

    public static void writeInteger(ByteBuf buffer, int value, int length) {
        for (int i = 0; i < length; i++) {
            buffer.writeByte(0x000000FF & (value >>> (i << 3)));
        }
    }

    public static void writeLong(ByteBuf buffer, long value, int length) {
        for (int i = 0; i < length; i++) {
            buffer.writeByte((int) (0x00000000000000FF & (value >>> (i << 3))));
        }
    }

    public static final void writeUB3(ByteBuf buffer, int i) {
        buffer.writeByte((byte) (i & 0xff));
        buffer.writeByte((byte) (i >>> 8));
        buffer.writeByte((byte) (i >>> 16));
    }

    public static final void writeInt(ByteBuf buffer, int i) {
        buffer.writeByte((byte) (i & 0xff));
        buffer.writeByte((byte) (i >>> 8));
        buffer.writeByte((byte) (i >>> 16));
        buffer.writeByte((byte) (i >>> 24));
    }

    public static final void writeFloat(ByteBuf buffer, float f) {
        writeInt(buffer, Float.floatToIntBits(f));
    }

    public static final void writeUB4(ByteBuf buffer, long l) {
        buffer.writeByte((byte) (l & 0xff));
        buffer.writeByte((byte) (l >>> 8));
        buffer.writeByte((byte) (l >>> 16));
        buffer.writeByte((byte) (l >>> 24));
    }

    public static final void writeLong(ByteBuf buffer, long l) {
        buffer.writeByte((byte) (l & 0xff));
        buffer.writeByte((byte) (l >>> 8));
        buffer.writeByte((byte) (l >>> 16));
        buffer.writeByte((byte) (l >>> 24));
        buffer.writeByte((byte) (l >>> 32));
        buffer.writeByte((byte) (l >>> 40));
        buffer.writeByte((byte) (l >>> 48));
        buffer.writeByte((byte) (l >>> 56));
    }

    public static final void writeDouble(ByteBuf buffer, double d) {
        writeLong(buffer, Double.doubleToLongBits(d));
    }

    /**
     * 二进制数据：数据长度不固定，长度值由数据前的1-9个字节决定：
     * [0,251)      1个字节存储数据长度
     * [251,2^16)   2个字节存储数据长度
     * [2^16,2^24)  3个字节存储数据长度
     * [2^24,2^64)  8个字节存储数据长度
     *
     * 以上述的方式写入实际该字节数据的长度值
     * @param buffer
     * @param l
     */
    public static final void writeLength(ByteBuf buffer, long l) {
        if (l < 251) {
            buffer.writeByte((byte) l);
        } else if (l < 0x10000L) {      //2^16
            buffer.writeByte((byte) 252);
            writeUB2(buffer, (int) l);
        } else if (l < 0x1000000L) {    //2^24
            buffer.writeByte((byte) 253);
            writeUB3(buffer, (int) l);
        } else {                        //2^64
            buffer.writeByte((byte) 254);
            writeLong(buffer, l);
        }
    }

    public static final void writeWithNull(ByteBuf buffer, byte[] src) {
        buffer.writeBytes(src);
        buffer.writeByte((byte) 0);
    }

    /**
     * 写下记录长度的字节数据 + 真实数据
     * @param buffer
     * @param src
     */
    public static final void writeWithLength(ByteBuf buffer, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buffer.writeByte((byte) length);
        } else if (length < 0x10000L) {
            buffer.writeByte((byte) 252);
            writeUB2(buffer, length);
        } else if (length < 0x1000000L) {
            buffer.writeByte((byte) 253);
            writeUB3(buffer, length);
        } else {
            buffer.writeByte((byte) 254);
            writeLong(buffer, length);
        }

        buffer.writeBytes(src);
    }

    public static final void writeWithLength(ByteBuf buffer, byte[] src, byte nullValue) {
        if (src == null) {
            buffer.writeByte(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }

    /**
     * @param length
     * @return 返回存储数据长度的字节个数
     */
    public static final int getLength(long length) {
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

    public static final int getLength(byte[] src) {
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
}
