package edu.uob;

public class DeleteStatement implements SQLStatement {
    private String tableName;
    private Condition condition; // 可为 null

    public DeleteStatement(String tableName, Condition condition) {
        this.tableName = tableName;
        this.condition = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public Condition getCondition() {
        return condition;
    }
}
