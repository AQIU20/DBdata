package edu.uob;

import edu.uob.SQLStatement;

import java.util.Map;

public class UpdateStatement implements SQLStatement {
    private final String tableName;
    private final Map<String, String> assignments;
    private final String whereColumn;
    private final String whereValue;

    public UpdateStatement(String tableName, Map<String, String> assignments, String whereColumn, String whereValue) {
        this.tableName = tableName;
        this.assignments = assignments;
        this.whereColumn = whereColumn;
        this.whereValue = whereValue;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getAssignments() {
        return assignments;
    }

    public String getWhereColumn() {
        return whereColumn;
    }

    public String getWhereValue() {
        return whereValue;
    }
}
