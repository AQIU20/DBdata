package edu.uob;

import java.io.File;


public class DatabaseManager {
    private static String currentDatabase = null;

    public static String getCurrentDatabase() {
        return currentDatabase;
    }

    //Create a new database directory.

    public static String createDatabase(String name) {
        boolean created = edu.uob.StorageManager.createDatabase(name);
        if (!created) {
            return edu.uob.ErrorHandler.databaseAlreadyExists(name);
        }
        return "";
    }

    //Drop delete an existing database.

    public static String dropDatabase(String name) {
        boolean deleted = edu.uob.StorageManager.deleteDatabase(name);
        if (!deleted) {
            return edu.uob.ErrorHandler.databaseNotFound(name);
        }
        // If the current database was this one, clear current selection
        if (name.equalsIgnoreCase(currentDatabase)) {
            currentDatabase = null;
        }
        return "";
    }

    //Use switch to a database as current.

    public static String useDatabase(String name) {
        if (!edu.uob.StorageManager.databaseExists(name)) {
            return edu.uob.ErrorHandler.databaseNotFound(name);
        }
        currentDatabase = name;
        return "";
    }
}
