package edu.uob;

public class DropTableStatement implements edu.uob.SQLStatement {
    private final String tableName;

    public DropTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}

