package edu.uob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser {

    public SQLStatement parse(List<String> tokens) throws Exception {
        if (tokens == null || tokens.isEmpty()) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        // Determine the type of statement from the first token
        String firstToken = tokens.get(0);
        switch (firstToken) {
            case "CREATE":
                return parseCreate(tokens);
            case "DROP":
                return parseDrop(tokens);
            case "USE":
                return parseUse(tokens);
            case "INSERT":
                return parseInsert(tokens);
            case "SELECT":
                return parseSelect(tokens);
            case "UPDATE":
                return parseUpdate(tokens);
            case "DELETE":
                return parseDelete(tokens);
            default:
                throw new Exception(ErrorHandler.syntaxError());
        }
    }

    private SQLStatement parseCreate(List<String> tokens) throws Exception {
        // e.g. CREATE DATABASE <name>  OR  CREATE TABLE <name> (...)
        if (tokens.size() < 3) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String second = tokens.get(1);
        if ("DATABASE".equals(second)) {
            if (tokens.size() != 3) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            String dbName = tokens.get(2);
            return new CreateDatabaseStatement(dbName);
        } else if ("TABLE".equals(second)) {
            return parseCreateTable(tokens);
        } else {
            throw new Exception(ErrorHandler.syntaxError());
        }
    }

    private CreateTableStatement parseCreateTable(List<String> tokens) throws Exception {
        // tokens example: ["CREATE","TABLE","tableName","(", "COL1", "INT", "PRIMARY", "KEY", ",", "COL2", "TEXT", ",", "COL3", "INT", ")"]
        if (tokens.size() < 4) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(2);
        // Expect "(" at tokens[3]
        if (!tokens.get(3).equals("(")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        List<ColumnDefinition> columns = new ArrayList<>();
        boolean primaryKeyFound = false;
        String primaryKeyColumn = null;
        // iterate from index 4 until we find the closing ")"
        int i = 4;
        while (i < tokens.size()) {
            String token = tokens.get(i);
            if (token.equals(")")) {
                break;
            }
            if (token.isEmpty() || token.equals(",")) {
                i++;
                continue;
            }
            if (token.equals("PRIMARY")) {
                // separate primary key clause: "PRIMARY KEY ( colName )"
                if (primaryKeyFound) {
                    throw new Exception(ErrorHandler.duplicatePrimaryKeyDefinition());
                }
                // next should be "KEY"
                if (i + 1 >= tokens.size() || !tokens.get(i + 1).equals("KEY")) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                // next should be "("
                if (i + 2 >= tokens.size() || !tokens.get(i + 2).equals("(")) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                // next is the column name, then ")"
                if (i + 3 >= tokens.size()) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                String pkColName = tokens.get(i + 3);
                if (i + 4 >= tokens.size() || !tokens.get(i + 4).equals(")")) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                primaryKeyFound = true;
                primaryKeyColumn = pkColName;
                i += 5; // skip "PRIMARY", "KEY", "(", colName, ")"
            } else {
                // parse a column definition
                String colName = token;
                // next token(s) should be type
                if (i + 1 >= tokens.size()) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                StringBuilder typeBuilder = new StringBuilder();
                int j = i + 1;
                while (j < tokens.size()) {
                    String tok = tokens.get(j);
                    if (tok.equals(",") || tok.equals(")") || tok.equals("PRIMARY")) {
                        break;
                    }
                    typeBuilder.append(tok);
                    if (j + 1 < tokens.size() && !tokens.get(j+1).equals(",")
                            && !tokens.get(j+1).equals(")")
                            && !tokens.get(j+1).equals("PRIMARY")) {
                        typeBuilder.append(" ");
                    }
                    j++;
                }
                String colType = typeBuilder.toString();
                boolean colPrimaryKey = false;
                if (j < tokens.size() && tokens.get(j).equals("PRIMARY")) {
                    // Inline primary key definition "PRIMARY KEY" after type
                    if (primaryKeyFound) {
                        // already have a primary key defined
                        throw new Exception(ErrorHandler.duplicatePrimaryKeyDefinition());
                    }
                    // ensure next is "KEY"
                    if (j + 1 >= tokens.size() || !tokens.get(j + 1).equals("KEY")) {
                        throw new Exception(ErrorHandler.syntaxError());
                    }
                    primaryKeyFound = true;
                    colPrimaryKey = true;
                    primaryKeyColumn = colName;
                    j += 2; // skip "PRIMARY" and "KEY"
                }
                columns.add(new ColumnDefinition(colName, colType, colPrimaryKey));
                i = j;
            }
        }
        if (i >= tokens.size() || !tokens.get(i).equals(")")) {
            // no closing parenthesis
            throw new Exception(ErrorHandler.syntaxError());
        }
        // If a separate PRIMARY KEY clause was given, mark that column in definitions
        if (primaryKeyFound && primaryKeyColumn != null) {
            boolean pkColumnExists = false;
            for (ColumnDefinition colDef : columns) {
                if (colDef.getName().equals(primaryKeyColumn)) {
                    colDef.setPrimaryKey(true);
                    pkColumnExists = true;
                }
            }
            if (!pkColumnExists) {
                throw new Exception(ErrorHandler.primaryKeyColumnNotFound(primaryKeyColumn));
            }
        }
        if (columns.isEmpty()) {
            throw new Exception(ErrorHandler.syntaxError()); // no columns defined
        }
        return new CreateTableStatement(tableName, columns);
    }

    private SQLStatement parseDrop(List<String> tokens) throws Exception {
        // DROP DATABASE <name> or DROP TABLE <name>
        if (tokens.size() < 3) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String type = tokens.get(1);
        String name = tokens.get(2);
        if ("DATABASE".equals(type)) {
            if (tokens.size() != 3) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            return new DropDatabaseStatement(name);
        } else if ("TABLE".equals(type)) {
            if (tokens.size() != 3) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            return new DropTableStatement(name);
        } else {
            throw new Exception(ErrorHandler.syntaxError());
        }
    }

    private SQLStatement parseUse(List<String> tokens) throws Exception {
        // USE <dbName>
        if (tokens.size() != 2) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        return new UseDatabaseStatement(tokens.get(1));
    }

    private InsertStatement parseInsert(List<String> tokens) throws Exception {
        // Example tokens: ["INSERT","INTO","tableName","VALUES","(", "val1", ",", "val2", ",", "'VAL 3'", ")"]
        if (tokens.size() < 6) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        if (!tokens.get(1).equals("INTO")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(2);
        if (!tokens.get(3).equals("VALUES")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        // Expect "(" at tokens[4]
        if (!tokens.get(4).equals("(")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        // Collect values until ")"
        List<String> values = new ArrayList<>();
        for (int i = 5; i < tokens.size(); i++) {
            String token = tokens.get(i);
            if (token.equals(")")) {
                break;
            }
            if (token.equals(",")) {
                continue;
            }
            values.add(token);
        }
        if (values.isEmpty()) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        return new InsertStatement(tableName, values);
    }

    private SelectStatement parseSelect(List<String> tokens) throws Exception {
        // Example tokens: ["SELECT", "*", "FROM", "tableName", "WHERE", "COL", "==", "'Simon'"]
        if (tokens.size() < 4) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        List<String> selectColumns = new ArrayList<>();
        int index = 1;
        // If first select field is "*"
        if (tokens.get(index).equals("*")) {
            selectColumns = null; // null or empty indicates all columns
            index++;
        } else {
            // parse column list until we see "FROM"
            while (index < tokens.size() && !tokens.get(index).equals("FROM")) {
                if (tokens.get(index).equals(",")) {
                    index++;
                    continue;
                }
                selectColumns.add(tokens.get(index));
                index++;
            }
        }
        if (index >= tokens.size() || !tokens.get(index).equals("FROM")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        index++;
        if (index >= tokens.size()) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(index++);
        String whereColumn = null;
        String whereValue = null;
        if (index < tokens.size()) {
            // If there's a WHERE clause
            if (!tokens.get(index).equals("WHERE")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            if (index + 3 >= tokens.size()) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            whereColumn = tokens.get(index + 1);
            String op = tokens.get(index + 2);
            // 允许 = 或 ==
            if (!op.equals("=") && !op.equals("==")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            whereValue = tokens.get(index + 3);
            // 如果 whereValue 有引号，通常在 Tokenizer 已去除，这里假设已去除
        }
        return new SelectStatement(tableName, selectColumns, whereColumn, whereValue);
    }

    private UpdateStatement parseUpdate(List<String> tokens) throws Exception {
        // Example tokens: ["UPDATE","tableName","SET","COL1","==","VAL1",",","COL2","=","VAL2","WHERE","COLX","==","VALX"]
        if (tokens.size() < 5) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(1);
        if (!tokens.get(2).equals("SET")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        // Parse assignments
        Map<String, String> assignments = new HashMap<>();
        int index = 3;
        while (index < tokens.size() && !tokens.get(index).equals("WHERE")) {
            // expecting column name, "==" or "=", value sequence, possibly multiple separated by comma
            String colName = tokens.get(index);
            if (colName.equals(",")) {
                index++;
                continue;
            }
            if (index + 1 >= tokens.size()) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            String op = tokens.get(index + 1);
            // 允许 = 或 ==
            if (!op.equals("=") && !op.equals("==")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            if (index + 2 >= tokens.size()) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            String value = tokens.get(index + 2);
            assignments.put(colName, value);
            index += 3;
        }
        String whereColumn = null;
        String whereValue = null;
        if (index < tokens.size()) {
            // parse WHERE clause
            if (!tokens.get(index).equals("WHERE") || index + 3 >= tokens.size()) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            whereColumn = tokens.get(index + 1);
            String op = tokens.get(index + 2);
            // 允许 = 或 ==
            if (!op.equals("=") && !op.equals("==")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            whereValue = tokens.get(index + 3);
        }
        if (assignments.isEmpty()) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        return new UpdateStatement(tableName, assignments, whereColumn, whereValue);
    }

    private DeleteStatement parseDelete(List<String> tokens) throws Exception {
        // Example tokens: ["DELETE","FROM","tableName","WHERE","COL","==","VAL"]
        if (tokens.size() < 3) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        int index = 1;
        if (!tokens.get(index).equals("FROM")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        index++;
        if (index >= tokens.size()) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(index++);
        String whereColumn = null;
        String whereValue = null;
        if (index < tokens.size()) {
            if (!tokens.get(index).equals("WHERE") || index + 3 >= tokens.size()) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            whereColumn = tokens.get(index + 1);
            String op = tokens.get(index + 2);
            // 允许 = 或 ==
            if (!op.equals("=") && !op.equals("==")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            whereValue = tokens.get(index + 3);
        }
        return new DeleteStatement(tableName, whereColumn, whereValue);
    }
}
