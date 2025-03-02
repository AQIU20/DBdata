package edu.uob;

import edu.uob.ColumnDefinition;

import java.util.List;

public class CreateTableStatement implements edu.uob.SQLStatement {
    private final String tableName;
    private final List<ColumnDefinition> columns;

    public CreateTableStatement(String tableName, List<ColumnDefinition> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }
}
