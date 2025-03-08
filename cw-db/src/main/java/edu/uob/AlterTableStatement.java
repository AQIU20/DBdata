package edu.uob;

public class AlterTableStatement implements SQLStatement {
    private String tableName;
    private String alterationType; // "ADD" 或 "DROP"
    private String attributeName;

    public AlterTableStatement(String tableName, String alterationType, String attributeName) {
        this.tableName = tableName;
        this.alterationType = alterationType;
        this.attributeName = attributeName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlterationType() {
        return alterationType;
    }

    public String getAttributeName() {
        return attributeName;
    }
}
