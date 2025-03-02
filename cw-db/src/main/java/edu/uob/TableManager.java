package edu.uob;

import edu.uob.ColumnDefinition;
import edu.uob.DatabaseManager;
import edu.uob.ErrorHandler;
import edu.uob.StorageManager;

import java.util.List;

public class TableManager {
    /**
     * 表管理
     * Create a new table file in the current database.
     */
    public static String createTable(String tableName, List<ColumnDefinition> columns) {
        // Build schema line to store in file
        StringBuilder schemaLine = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition col = columns.get(i);
            schemaLine.append(col.getName()).append(" ").append(col.getType());
            if (col.isPrimaryKey()) {
                schemaLine.append(" PRIMARY KEY");
            }
            if (i < columns.size() - 1) {
                schemaLine.append(", ");
            }
        }
        boolean created = StorageManager.createTable(DatabaseManager.getCurrentDatabase(), tableName, schemaLine.toString());
        if (!created) {
            return ErrorHandler.tableAlreadyExists(tableName);
        }
        return "Table " + tableName + " created.";
    }

    /**
     * Drop (delete) a table from the current database.
     */
    public static String dropTable(String tableName) {
        boolean deleted = StorageManager.deleteTable(DatabaseManager.getCurrentDatabase(), tableName);
        if (!deleted) {
            return ErrorHandler.tableNotFound(tableName);
        }
        return "Table " + tableName + " deleted.";
    }
}

