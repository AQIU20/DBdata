package edu.uob;

import java.util.List;

public class SelectStatement implements SQLStatement {
    private String tableName;
    private List<String> columns;   // null 表示选择所有列
    private Condition condition;    // 可以为 null，表示没有 WHERE

    public SelectStatement(String tableName, List<String> columns, Condition condition) {
        this.tableName = tableName;
        this.columns = columns;
        this.condition = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Condition getCondition() {
        return condition;
    }
}
