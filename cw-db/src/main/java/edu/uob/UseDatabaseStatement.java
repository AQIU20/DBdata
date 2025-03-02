package edu.uob;

import edu.uob.SQLStatement;

public class UseDatabaseStatement implements SQLStatement {
    private final String databaseName;

    public UseDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
