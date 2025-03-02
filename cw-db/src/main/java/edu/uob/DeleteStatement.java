package edu.uob;

public class DeleteStatement implements edu.uob.SQLStatement {
    private final String tableName;
    private final String whereColumn;
    private final String whereValue;

    public DeleteStatement(String tableName, String whereColumn, String whereValue) {
        this.tableName = tableName;
        this.whereColumn = whereColumn;
        this.whereValue = whereValue;
    }

    public String getTableName() {
        return tableName;
    }

    public String getWhereColumn() {
        return whereColumn;
    }

    public String getWhereValue() {
        return whereValue;
    }
}
