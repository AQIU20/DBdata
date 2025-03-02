package edu.uob;

public class ErrorHandler {
    public static String syntaxError() {
        return "ERROR: Syntax error.";
    }
    public static String databaseAlreadyExists(String name) {
        return "ERROR: Database " + name + " already exists.";
    }
    public static String databaseNotFound(String name) {
        return "ERROR: Database " + name + " does not exist.";
    }
    public static String tableAlreadyExists(String name) {
        return "ERROR: Table " + name + " already exists.";
    }
    public static String tableNotFound(String name) {
        return "ERROR: Table " + name + " does not exist.";
    }
    public static String noDatabaseSelected() {
        return "ERROR: No database selected.";
    }
    public static String duplicatePrimaryKeyDefinition() {
        return "ERROR: Multiple primary key definitions.";
    }
    public static String multiplePrimaryKeys() {
        return "ERROR: Multiple primary keys defined.";
    }
    public static String primaryKeyColumnNotFound(String colName) {
        return "ERROR: Primary key column " + colName + " not found in table definition.";
    }
    public static String duplicateColumnName(String colName) {
        return "ERROR: Duplicate column name " + colName + ".";
    }
    public static String columnCountMismatch() {
        return "ERROR: Column count does not match.";
    }
    public static String duplicatePrimaryKeyValue(String value) {
        return "ERROR: Duplicate primary key value " + value + ".";
    }
    public static String typeMismatch(String colName, String expectedType) {
        return "ERROR: Type mismatch for column " + colName + " (expected " + expectedType + ").";
    }
    public static String columnNotFound(String colName) {
        return "ERROR: Column " + colName + " does not exist.";
    }
    public static String generalError(String message) {
        return "ERROR: " + message;
    }
}
