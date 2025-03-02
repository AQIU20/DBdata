package edu.uob;

import java.util.List;

public class SelectStatement implements edu.uob.SQLStatement {
    private final String tableName;
    private final List<String> columns; // null or empty for all columns
    private final String whereColumn;
    private final String whereValue;

    public SelectStatement(String tableName, List<String> columns, String whereColumn, String whereValue) {
        this.tableName = tableName;
        this.columns = columns;
        this.whereColumn = whereColumn;
        this.whereValue = whereValue;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getWhereColumn() {
        return whereColumn;
    }

    public String getWhereValue() {
        return whereValue;
    }
}

