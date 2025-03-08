package edu.uob;

import java.util.Map;

public class UpdateStatement implements SQLStatement {
    private String tableName;
    private Map<String, String> assignments;
    private Condition condition;  // 可为 null

    public UpdateStatement(String tableName, Map<String, String> assignments, Condition condition) {
        this.tableName = tableName;
        this.assignments = assignments;
        this.condition = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getAssignments() {
        return assignments;
    }

    public Condition getCondition() {
        return condition;
    }
}
