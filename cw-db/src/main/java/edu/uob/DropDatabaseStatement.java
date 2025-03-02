package edu.uob;

public class DropDatabaseStatement implements edu.uob.SQLStatement {
    private final String databaseName;

    public DropDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
