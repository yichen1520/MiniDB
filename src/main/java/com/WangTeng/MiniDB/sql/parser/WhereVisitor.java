package com.WangTeng.MiniDB.sql.parser;

import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueLong;
import com.WangTeng.MiniDB.meta.value.ValueString;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.HashMap;
import java.util.Map;

public class WhereVisitor extends SQLASTVisitorAdapter {
    // property的等于限定值
    private Map<SQLExpr, Value> equalMap = new HashMap<>();
    // property的最大值
    private Map<SQLExpr, Value> lessOrEqualMap = new HashMap<>();
    // property的等于值
    private Map<SQLExpr, Value> greaterOrEqualMap = new HashMap<>();
    // 如果出现or condition则直接走顺序扫描
    private boolean hasOr;
    // 是否冲突
    private boolean isConflict;

    public boolean visit(SQLBinaryOpExpr expr) {
        SQLExpr left = expr.getLeft();
        SQLExpr right = expr.getRight();
        switch (expr.getOperator()) {
            case BooleanOr:
                hasOr = true;
                break;
            case Equality:
                if ((left instanceof SQLPropertyExpr || left instanceof SQLIdentifierExpr)
                        && (right instanceof SQLLiteralExpr)) { // right是常量表达式
                    equalMapSet(left, getValue((SQLLiteralExpr) right));
                }
                break;
            case LessThan:
            case LessThanOrEqual:
                if ((left instanceof SQLPropertyExpr || left instanceof SQLIdentifierExpr)
                        && (right instanceof SQLLiteralExpr)) {
                    lessOrEqualMapSet(left, getValue((SQLLiteralExpr) right));
                }
                break;
            case GreaterThan:
            case GreaterThanOrEqual:
                if ((left instanceof SQLPropertyExpr || left instanceof SQLIdentifierExpr)
                        && (right instanceof SQLLiteralExpr)) {
                    greaterOrEqualMapSet(left, getValue((SQLLiteralExpr) right));
                }
                break;
            default:
                break;
        }
        return true;
    }

    public void greaterOrEqualMapSet(SQLExpr expr, Value value) {
        Value oldValue = greaterOrEqualMap.get(expr);
        if (oldValue != null) {
            if (oldValue.compare(value) == 0) {
                return;
            } else if (oldValue.compare(value) < 0) {
                greaterOrEqualMap.put(expr, value);
            }
        } else {
            greaterOrEqualMap.put(expr, value);
        }
    }

    public void lessOrEqualMapSet(SQLExpr expr, Value value) {
        Value oldValue = lessOrEqualMap.get(expr);
        if (oldValue != null) {
            if (oldValue.compare(value) == 0) {
                return;
            } else if (oldValue.compare(value) > 0) {
                lessOrEqualMap.put(expr, value);
            }
        } else {
            lessOrEqualMap.put(expr, value);
        }
    }

    public Value getValue(SQLLiteralExpr right) {
        Value value = null;
        if (right instanceof SQLTextLiteralExpr) {
            value = new ValueString(((SQLTextLiteralExpr) right).getText());
        } else if (right instanceof SQLNumericLiteralExpr) {
            value = new ValueLong(((SQLNumericLiteralExpr) right).getNumber().longValue());
        }
        return value;
    }

    public void equalMapSet(SQLExpr expr, Value value) {
        Value oldValue = equalMap.get(expr);
        if (oldValue != null) {
            if (oldValue.compare(value) == 0) {
                return;
            } else {
                // 设置为冲突,表明无法得到值
                isConflict = true;
            }
        } else {
            equalMap.put(expr, value);
        }
    }

    public Map<SQLExpr, Value> getEqualMap() {
        return equalMap;
    }

    public void setEqualMap(
            Map<SQLExpr, Value> equalMap) {
        this.equalMap = equalMap;
    }

    public Map<SQLExpr, Value> getLessOrEqualMap() {
        return lessOrEqualMap;
    }

    public void setLessOrEqualMap(
            Map<SQLExpr, Value> lessOrEqualMap) {
        this.lessOrEqualMap = lessOrEqualMap;
    }

    public Map<SQLExpr, Value> getGreaterOrEqualMap() {
        return greaterOrEqualMap;
    }

    public void setGreaterOrEqualMap(
            Map<SQLExpr, Value> greaterOrEqualMap) {
        this.greaterOrEqualMap = greaterOrEqualMap;
    }

    public boolean isHasOr() {
        return hasOr;
    }

    public void setHasOr(boolean hasOr) {
        this.hasOr = hasOr;
    }

    public boolean isConflict() {
        return isConflict;
    }

    public void setConflict(boolean conflict) {
        isConflict = conflict;
    }
}
