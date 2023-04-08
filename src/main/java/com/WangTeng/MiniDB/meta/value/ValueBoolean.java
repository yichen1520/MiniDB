package com.WangTeng.MiniDB.meta.value;

public class ValueBoolean extends Value {

    private boolean b;

    public ValueBoolean() {
    }

    public ValueBoolean(boolean b) {
        this.b = b;
    }

    @Override
    public int getLength() {
        return 1 + 1;
    }

    @Override
    public byte getType() {
        return BOOLEAN;
    }

    // [type][data] 1为true;2为false
    @Override
    public byte[] getBytes() {
        byte[] result = new byte[2];
        result[0] = BOOLEAN;
        if (b) {
            result[1] = 1;
        } else {
            result[1] = 0;
        }
        return result;
    }

    @Override
    public void read(byte[] bytes) {
        if (bytes[0] == 0) {
            b = false;
        } else {
            b = true;
        }
    }

    @Override
    public String getString() {
        return toString();
    }

    @Override
    public String toString() {
        if (b) {
            return "true";
        } else {
            return "false";
        }
    }

    public boolean getBoolean() {
        return b;
    }

    public ValueBoolean setBoolean(boolean b) {
        this.b = b;
        return this;
    }

    @Override
    public int compare(Value value) {
        boolean toCompare = ((ValueBoolean) value).getBoolean();
        if (b) {
            if (toCompare) {
                return 0;
            } else {
                return 1;
            }
        } else {
            if (toCompare) {
                return -1;
            } else {
                return 0;
            }
        }

    }
}
