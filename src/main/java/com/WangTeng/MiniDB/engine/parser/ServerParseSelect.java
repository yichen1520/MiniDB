package com.WangTeng.MiniDB.engine.parser;

import com.WangTeng.MiniDB.engine.parser.util.CharTypes;
import com.WangTeng.MiniDB.engine.parser.util.ParseUtil;

public class ServerParseSelect {
    public static final int OTHER = -1;
    public static final int VERSION_COMMENT = 1;
    public static final int DATABASE = 2;
    public static final int USER = 3;
    public static final int LAST_INSERT_ID = 4;
    public static final int IDENTITY = 5;
    public static final int VERSION = 6;
    public static final int TX_ISOLATION = 7;
    public static final int AUTO_INCREMENT = 8;

    private static final char[] _VERSION_COMMENT = "VERSION_COMMENT".toCharArray();
    private static final char[] _IDENTITY = "IDENTITY".toCharArray();
    private static final char[] _LAST_INSERT_ID = "LAST_INSERT_ID".toCharArray();
    private static final char[] _DATABASE = "DATABASE()".toCharArray();

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case '@':
                    return select2Check(stmt, i);
                case 'D':
                case 'd':
                    return databaseCheck(stmt, i);
                case 'L':
                case 'l':
                    return lastInsertCheck(stmt, i);
                case 'U':
                case 'u':
                    return userCheck(stmt, i);
                case 'V':
                case 'v':
                    return versionCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SELECT VERSION
    private static int versionCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ERSION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r') && (c3 == 'S' || c3 == 's')
                    && (c4 == 'I' || c4 == 'i') && (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            continue;
                        case '(':
                            return versionParenthesisCheck(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    /**
     *SELECT VERSION (  右括号检查
     */
    private static int versionParenthesisCheck(String stmt, int offset) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case ')':
                    while (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\t':
                            case '\r':
                            case '\n':
                                continue;
                            default:
                                return OTHER;
                        }
                    }
                    return VERSION;
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    /**
     * <code>SELECT LAST_INSERT_ID() AS id, </code>
     */
    private static int skipAlias(String stmt, int offset) {
        offset = ParseUtil.move(stmt, offset, 0);
        if (offset >= stmt.length()) {
            return offset;
        }
        switch (stmt.charAt(offset)) {
            case '\'':
                return skipString(stmt, offset);
            case '"':
                return skipString2(stmt, offset);
            case '`':
                return skipIdentifierEscape(stmt, offset);
            default:
                if (CharTypes.isIdentifierChar(stmt.charAt(offset))) {
                    for (; offset < stmt.length() && CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset) {}
                    return offset;
                }
        }
        return -1;
    }

    /**
     * <code>`abc`d</code>
     */
    private static int skipIdentifierEscape(String stmt, int offset) {
        for (++offset; offset < stmt.length(); ++offset) {
            if (stmt.charAt(offset) == '`') {
                if (++offset >= stmt.length() || stmt.charAt(offset) != '`') {
                    return offset;
                }
            }
        }
        return -1;
    }

    /**
     * 使用状态机跳过 " 字符，返回该字符下一偏移量，同时忽略'\\'字符后的第一个 " 字符
     */
    private static int skipString2(String stmt, int offset) {
        int state = 0;
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            switch (state) {
                case 0:
                    switch (c) {
                        case '\\':
                            state = 1;
                            break;
                        case '"':
                            state = 2;
                            break;
                    }
                    break;
                case 1:
                    state = 0;
                    break;
                case 2:
                    switch (c) {
                        case '"':
                            state = 0;
                            break;
                        default:
                            return offset;
                    }
                    break;
            }
        }
        if (offset == stmt.length() && state == 2) {
            return stmt.length();
        }
        return -1;
    }

    /**
     * 使用状态机跳过 ' 字符，返回该字符下一偏移量，同时忽略'\\'字符后的第一个 ' 字符
     */
    private static int skipString(String stmt, int offset) {
        int state = 0;
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            switch (state) {
                case 0:
                    switch (c) {
                        case '\\':
                            state = 1;
                            break;
                        case '\'':
                            state = 2;
                            break;
                    }
                    break;
                case 1:
                    state = 0;
                    break;
                case 2:
                    switch (c) {
                        case '\'':
                            state = 0;
                            break;
                        default:
                            return offset;
                    }
                    break;
            }
        }
        if (offset == stmt.length() && state == 2) {
            return stmt.length();
        }
        return -1;
    }

    /**
     * <code>SELECT LAST_INSERT_ID() AS id</code>
     */
    public static int skipAs(String stmt, int offset) {
        offset = ParseUtil.move(stmt, offset, 0);
        if (stmt.length() > offset + "AS".length()
                && (stmt.charAt(offset) == 'A' || stmt.charAt(offset) == 'a')
                && (stmt.charAt(offset + 1) == 'S' || stmt.charAt(offset + 1) == 's')
                && (stmt.charAt(offset + 2) == ' ' || stmt.charAt(offset + 2) == '\r'
                || stmt.charAt(offset + 2) == '\n' || stmt.charAt(offset + 2) == '\t'
                || stmt.charAt(offset + 2) == '/' || stmt.charAt(offset + 2) == '#')) {
            offset = ParseUtil.move(stmt, offset + 2, 0);
        }
        return offset;
    }

    /**
     * 找到LAST_INSERT_ID()函数
     */
    public static int indexAfterLastInsertIdFunc(String stmt, int offset) {
        if (stmt.length() >= offset + "LAST_INSERT_ID()".length()) {
            if (ParseUtil.compare(stmt, offset, _LAST_INSERT_ID)) {
                offset = ParseUtil.move(stmt, offset + _LAST_INSERT_ID.length, 0);
                if (offset + 1 < stmt.length() && stmt.charAt(offset) == '(') {
                    offset = ParseUtil.move(stmt, offset + 1, 0);
                    if (offset < stmt.length() && stmt.charAt(offset) == ')') {
                        return ++offset;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * 查找是否存在IDENTITY关键字附近的行索引
     * 行索引是用于唯一标识表中每一行数据的标识符。
     * 在创建表时，如果使用了IDENTITY关键字，则 MySQL 将在表创建时自动生成主键值，并且同时为该主键值创建一个唯一的行索引
     * @param stmt
     * @param offset
     * @return 如果找到了IDENTITY关键字附近的行索引，则该方法将返回行索引;否则，它将返回 -1
     */
    public static int indexAfterIdentity(String stmt, int offset) {
        char first = stmt.charAt(offset);
        switch (first) {
            case '`':
            case '\'':
            case '"':
                if (stmt.length() < offset + "identity".length() + 2) {
                    return -1;
                }
                if (stmt.charAt(offset + "identity".length() + 1) != first) {
                    return -1;
                }
                ++offset;
                break;
            case 'i':
            case 'I':
                if (stmt.length() < offset + "identity".length()) {
                    return -1;
                }
                break;
            default:
                return -1;
        }
        if (ParseUtil.compare(stmt, offset, _IDENTITY)) {
            offset += _IDENTITY.length;
            switch (first) {
                case '`':
                case '\'':
                case '"':
                    return ++offset;
            }
            return offset;
        }
        return -1;
    }

    /**
     * SELECT LAST_INSERT_ID()
     */
    static int lastInsertCheck(String stmt, int offset) {
        offset = indexAfterLastInsertIdFunc(stmt, offset);  //拿到 LAST_INSERT_ID() 下一字节的偏移量
        if (offset < 0) {
            return OTHER;
        }
        offset = skipAs(stmt, offset);
        offset = skipAlias(stmt, offset);
        if (offset < 0) {
            return OTHER;
        }
        offset = ParseUtil.move(stmt, offset, 0);
        if (offset < stmt.length()) {
            return OTHER;
        }
        return LAST_INSERT_ID;
    }

    /**
     * select @@identity<br/>
     * select @@identiTy aS iD
     */
    static int identityCheck(String stmt, int offset) {
        offset = indexAfterIdentity(stmt, offset);
        if (offset < 0) {
            return OTHER;
        }
        offset = skipAs(stmt, offset);
        offset = skipAlias(stmt, offset);
        if (offset < 0) {
            return OTHER;
        }
        offset = ParseUtil.move(stmt, offset, 0);
        if (offset < stmt.length()) {
            return OTHER;
        }
        return IDENTITY;
    }

    static int select2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                    case 'V':
                    case 'v':
                        return versionCommentCheck(stmt, offset);
                    case 'i':
                    case 'I':
                        return identityCheck(stmt, offset);
                    default:
                        break;
                }
            }
        }
        if (stmt.toUpperCase().indexOf("TX_ISOLATION") >= 0) {
            return TX_ISOLATION;
        }
        if (stmt.toUpperCase().indexOf("AUTO_INCREMENT_INCREMENT") >= 0) {
            return AUTO_INCREMENT;
        }
        return OTHER;
    }

    /**
     * SELECT DATABASE()
     */
    static int databaseCheck(String stmt, int offset) {
        int length = offset + _DATABASE.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, _DATABASE)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            } else {
                return DATABASE;
            }
        }
        return OTHER;
    }

    /**
     * SELECT USER()
     */
    static int userCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r') && (c4 == '(')
                    && (c5 == ')') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return USER;
            }
        }
        return OTHER;
    }

    /**
     * SELECT @@VERSION_COMMENT
     * 获取当前数据库引擎的版本注释。
     */
    static int versionCommentCheck(String stmt, int offset) {
        int length = offset + _VERSION_COMMENT.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, _VERSION_COMMENT)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            } else {
                return VERSION_COMMENT;
            }
        }
        return OTHER;
    }
}
