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
        String firstToken = tokens.get(0);

        if ("CREATE".equals(firstToken)) {
            return parseCreate(tokens);
        } else if ("DROP".equals(firstToken)) {
            return parseDrop(tokens);
        } else if ("USE".equals(firstToken)) {
            return parseUse(tokens);
        } else if ("INSERT".equals(firstToken)) {
            return parseInsert(tokens);
        } else if ("SELECT".equals(firstToken)) {
            return parseSelect(tokens);
        } else if ("UPDATE".equals(firstToken)) {
            return parseUpdate(tokens);
        } else if ("DELETE".equals(firstToken)) {
            return parseDelete(tokens);
        } else if ("JOIN".equals(firstToken)) {
            return parseJoin(tokens);
        } else if ("ALTER".equals(firstToken)) {
            return parseAlter(tokens);
        } else {
            throw new Exception(ErrorHandler.syntaxError());
        }
    }

    private SQLStatement parseCreate(List<String> tokens) throws Exception {
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
        if (tokens.size() < 4) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(2);
        if (!tokens.get(3).equals("(")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        List<ColumnDefinition> columns = new ArrayList<>();
        boolean primaryKeyFound = false;
        String primaryKeyColumn = null;
        int i = 4;
        while (i < tokens.size()) {
            String token = tokens.get(i);
            if (token.equals(")")) {
                break;
            }
            if (token.isEmpty() || token.equals(",") || token.equals("\t")) {
                i++;
                continue;
            }
            if (token.equals("PRIMARY")) {
                if (primaryKeyFound) {
                    throw new Exception(ErrorHandler.duplicatePrimaryKeyDefinition());
                }
                if (i + 1 >= tokens.size() || !tokens.get(i + 1).equals("KEY")) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                if (i + 2 >= tokens.size() || !tokens.get(i + 2).equals("(")) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                if (i + 3 >= tokens.size()) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                String pkColName = tokens.get(i + 3);
                if (i + 4 >= tokens.size() || !tokens.get(i + 4).equals(")")) {
                    throw new Exception(ErrorHandler.syntaxError());
                }
                primaryKeyFound = true;
                primaryKeyColumn = pkColName;
                i += 5;
            } else {
                String colName = token;
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
                    if (primaryKeyFound) {
                        throw new Exception(ErrorHandler.duplicatePrimaryKeyDefinition());
                    }
                    if (j + 1 >= tokens.size() || !tokens.get(j + 1).equals("KEY")) {
                        throw new Exception(ErrorHandler.syntaxError());
                    }
                    primaryKeyFound = true;
                    colPrimaryKey = true;
                    primaryKeyColumn = colName;
                    j += 2;
                }
                columns.add(new ColumnDefinition(colName, colType, colPrimaryKey));
                i = j;
            }
        }
        if (i >= tokens.size() || !tokens.get(i).equals(")")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
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
            throw new Exception(ErrorHandler.syntaxError());
        }
        return new CreateTableStatement(tableName, columns);
    }

    private SQLStatement parseDrop(List<String> tokens) throws Exception {
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
        if (tokens.size() != 2) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        return new UseDatabaseStatement(tokens.get(1));
    }

    private InsertStatement parseInsert(List<String> tokens) throws Exception {
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
        if (!tokens.get(4).equals("(")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
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
        if (tokens.size() < 4) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        List<String> selectColumns = new ArrayList<>();
        int index = 1;
        if (tokens.get(index).equals("*")) {
            selectColumns = null;
            index++;
        } else {
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
        Condition condition = null;
        if (index < tokens.size() && tokens.get(index).equals("WHERE")) {
            condition = parseCondition(tokens, index + 1, tokens.size());
        }
        return new SelectStatement(tableName, selectColumns, condition);
    }

    private UpdateStatement parseUpdate(List<String> tokens) throws Exception {
        if (tokens.size() < 5) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(1);
        if (!tokens.get(2).equals("SET")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        Map<String, String> assignments = new HashMap<>();
        int index = 3;
        while (index < tokens.size() && !tokens.get(index).equals("WHERE")) {
            String colName = tokens.get(index);
            if (colName.equals(",")) {
                index++;
                continue;
            }
            if (index + 1 >= tokens.size() || (!tokens.get(index + 1).equals("=") && !tokens.get(index + 1).equals("=="))) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            if (index + 2 >= tokens.size()) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            String value = tokens.get(index + 2);
            assignments.put(colName, value);
            index += 3;
        }
        Condition condition = null;
        if (index < tokens.size()) {
            if (!tokens.get(index).equals("WHERE")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            condition = parseCondition(tokens, index + 1, tokens.size());
        }
        if (assignments.isEmpty()) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        return new UpdateStatement(tableName, assignments, condition);
    }

    private DeleteStatement parseDelete(List<String> tokens) throws Exception {
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
        Condition condition = null;
        if (index < tokens.size()) {
            if (!tokens.get(index).equals("WHERE")) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            condition = parseCondition(tokens, index + 1, tokens.size());
        }
        return new DeleteStatement(tableName, condition);
    }

    private Condition parseCondition(List<String> tokens, int start, int end) throws Exception {
        if (tokens.get(start).equals("(") && tokens.get(end - 1).equals(")")) {
            start++;
            end--;
        }
        int level = 0;
        int opIndex = -1;
        String boolOp = null;
        for (int i = start; i < end; i++) {
            String t = tokens.get(i);
            if (t.equals("(")) {
                level++;
            } else if (t.equals(")")) {
                level--;
            } else if (level == 0 && (t.equals("AND") || t.equals("OR"))) {
                opIndex = i;
                boolOp = t;
                break;
            }
        }
        if (opIndex != -1) {
            Condition left = parseCondition(tokens, start, opIndex);
            Condition right = parseCondition(tokens, opIndex + 1, end);
            return new CompoundCondition(left, boolOp, right);
        } else {
            if (end - start != 3) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            String attribute = tokens.get(start);
            String comparator = tokens.get(start + 1);
            if (!isValidComparator(comparator)) {
                throw new Exception(ErrorHandler.syntaxError());
            }
            String value = tokens.get(start + 2);
            return new SimpleCondition(attribute, comparator, value);
        }
    }

    private boolean isValidComparator(String op) {
        return op.equals("=") || op.equals("==") || op.equals(">") || op.equals("<") ||
                op.equals(">=") || op.equals("<=") || op.equals("!=") || op.equals("LIKE");
    }

    private JoinStatement parseJoin(List<String> tokens) throws Exception {
        if (tokens.size() != 8) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        if (!tokens.get(2).equals("AND")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        if (!tokens.get(4).equals("ON")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        if (!tokens.get(6).equals("AND")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String table1 = tokens.get(1);
        String table2 = tokens.get(3);
        String attr1 = tokens.get(5);
        String attr2 = tokens.get(7);
        return new JoinStatement(table1, table2, attr1, attr2);
    }

    private AlterTableStatement parseAlter(List<String> tokens) throws Exception {
        if (tokens.size() != 5) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        if (!tokens.get(1).equals("TABLE")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String tableName = tokens.get(2);
        String alterationType = tokens.get(3);
        if (!alterationType.equals("ADD") && !alterationType.equals("DROP")) {
            throw new Exception(ErrorHandler.syntaxError());
        }
        String attributeName = tokens.get(4);
        return new AlterTableStatement(tableName, alterationType, attributeName);
    }
}