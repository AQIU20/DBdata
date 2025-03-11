package edu.uob;

public class ErrorHandler {

    public static String syntaxError() {
        String error = "ERROR: ";
        error = error + "Syntax error";
        error = error + ".";
        return error;
    }

    public static String databaseAlreadyExists(String name) {
        String message = "ERROR: Database ";
        message = message + name;
        message = message + " already exists.";
        return message;
    }

    public static String databaseNotFound(String name) {
        String result = "ERROR: Database ";
        result += name;
        result += " does not exist.";
        return result;
    }

    public static String tableAlreadyExists(String name) {
        return "ERROR: Table " + name + " already exists.";
    }

    public static String tableNotFound(String name) {
        return "ERROR: Table " + name + " does not exist.";
    }

    public static String noDatabaseSelected() {
        String output = "";
        output = "ERROR: No database selected.";
        return output;
    }

    public static String duplicatePrimaryKeyDefinition() {
        String msg = "ERROR: Multiple primary key definitions.";
        return msg;
    }

    public static String multiplePrimaryKeys() {
        return "ERROR: Multiple primary keys defined.";
    }

    public static String primaryKeyColumnNotFound(String colName) {
        String temp = "ERROR: Primary key column ";
        temp = temp + colName;
        temp = temp + " not found in table definition.";
        return temp;
    }

    public static String duplicateColumnName(String colName) {
        return "ERROR: Duplicate column name " + colName + ".";
    }

    public static String columnCountMismatch() {
        return "ERROR: Column count does not match.";
    }

    public static String duplicatePrimaryKeyValue(String value) {
        String errorMsg = "ERROR: Duplicate primary key value ";
        errorMsg = errorMsg.concat(value);
        errorMsg = errorMsg.concat(".");
        return errorMsg;
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