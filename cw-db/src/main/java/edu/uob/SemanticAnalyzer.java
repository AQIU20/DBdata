package edu.uob;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SemanticAnalyzer {
    /**
     * 语义分析
     * Validate the SQL statement in the context of current database state.
     * Throws an exception with appropriate error message if a semantic rule is violated.
     */
    public void validate(edu.uob.SQLStatement statement) throws Exception {
        if (statement instanceof CreateDatabaseStatement) {
            CreateDatabaseStatement stmt = (CreateDatabaseStatement) statement;
            String dbName = stmt.getDatabaseName();
            // Check if database already exists
            if (edu.uob.StorageManager.databaseExists(dbName)) {
                throw new Exception(ErrorHandler.databaseAlreadyExists(dbName));
            }
        } else if (statement instanceof DropDatabaseStatement) {
            DropDatabaseStatement stmt = (DropDatabaseStatement) statement;
            String dbName = stmt.getDatabaseName();
            // Check if database existsList<ColumnDefinition>
            if (!edu.uob.StorageManager.databaseExists(dbName)) {
                throw new Exception(ErrorHandler.databaseNotFound(dbName));
            }
        } else if (statement instanceof edu.uob.UseDatabaseStatement) {
            edu.uob.UseDatabaseStatement stmt = (edu.uob.UseDatabaseStatement) statement;
            String dbName = stmt.getDatabaseName();
            if (!edu.uob.StorageManager.databaseExists(dbName)) {
                throw new Exception(ErrorHandler.databaseNotFound(dbName));
            }
        } else if (statement instanceof CreateTableStatement) {
            CreateTableStatement stmt = (CreateTableStatement) statement;
            String tableName = stmt.getTableName();
            // Ensure a database is selected
            if (DatabaseManager.getCurrentDatabase() == null) {
                throw new Exception(ErrorHandler.noDatabaseSelected());
            }
            // Check if table already exists
            if (edu.uob.StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
                throw new Exception(ErrorHandler.tableAlreadyExists(tableName));
            }
            // Check for duplicate column names
            List<ColumnDefinition> columns = stmt.getColumns();
            Set<String> colNames = new HashSet<>();
            for (ColumnDefinition col : columns) {
                if (colNames.contains(col.getName())) {
                    throw new Exception(ErrorHandler.duplicateColumnName(col.getName()));
                }
                colNames.add(col.getName());
            }
            // Check primary key constraints - only one primary key allowed (should be enforced by parser too)
            int pkCount = 0;
            for (ColumnDefinition col : columns) {
                if (col.isPrimaryKey()) pkCount++;
            }
            if (pkCount > 1) {
                throw new Exception(ErrorHandler.multiplePrimaryKeys());
            }
        } else if (statement instanceof DropTableStatement) {
            DropTableStatement stmt = (DropTableStatement) statement;
            String tableName = stmt.getTableName();
            if (DatabaseManager.getCurrentDatabase() == null) {
                throw new Exception(ErrorHandler.noDatabaseSelected());
            }
            if (!edu.uob.StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
                throw new Exception(ErrorHandler.tableNotFound(tableName));
            }
        } else if (statement instanceof InsertStatement) {
            InsertStatement stmt = (InsertStatement) statement;
            String tableName = stmt.getTableName();
            if (DatabaseManager.getCurrentDatabase() == null) {
                throw new Exception(ErrorHandler.noDatabaseSelected());
            }
            if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
                throw new Exception(ErrorHandler.tableNotFound(tableName));
            }
            // 获取用户提供的值
            List<String> values = stmt.getValues();
            // 读取表的 schema
            List<ColumnDefinition> columns = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
            if (columns == null) {
                throw new Exception(ErrorHandler.tableNotFound(tableName));
            }
            // 判断是否存在自动生成的 id 列（假设 id 列总是出现在第一位）
            int expectedColumns = columns.size();
            if (!columns.isEmpty() && columns.get(0).getName().equalsIgnoreCase("id")) {
                // 用户无需提供 id 的值，所以期望值数量为 schema 数量减 1
                expectedColumns = columns.size() - 1;
            }
            if (values.size() != expectedColumns) {
                System.err.println("[ERROR] Expected columns: " + expectedColumns);
                System.err.println("[ERROR] Provided values: " + values.size());
                System.err.flush();
                throw new Exception(ErrorHandler.columnCountMismatch());
            }
            // 检查主键唯一性
            int pkIndex = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).isPrimaryKey()) {
                    pkIndex = i;
                    break;
                }
            }
            if (pkIndex != -1) {
                String newPkValue;
                if (pkIndex == 0) {
                    // 此时 id 是自动生成的，不需要检测唯一性，因为 QueryExecutor 会生成唯一 id
                    newPkValue = "";
                } else {
                    newPkValue = values.get(pkIndex - 1); // 注意：由于 id 未提供，后续索引减 1
                }
                // 读取已有记录，检查主键重复（如果主键不是 id 则执行检查）
                if (!newPkValue.isEmpty()) {
                    List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
                    for (List<String> record : records) {
                        if (record.size() > pkIndex && record.get(pkIndex).equals(newPkValue)) {
                            throw new Exception(ErrorHandler.duplicatePrimaryKeyValue(newPkValue));
                        }
                    }
                }
            }
            // 检查类型匹配（例如 INT 类型必须为数字）
            // 注意：此处需要调整索引，因为如果存在自动 id 列，用户提供的值从 columns 索引 1 开始对应
            for (int i = 0; i < values.size(); i++) {
                // 实际对应的列为 i+1
                ColumnDefinition col = columns.get(i + 1);
                String value = values.get(i);
                if (col.getType().toUpperCase().startsWith("INT")) {
                    try {
                        Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new Exception(ErrorHandler.typeMismatch(col.getName(), col.getType()));
                    }
                }
            }
        } else if (statement instanceof SelectStatement) {
        SelectStatement stmt = (SelectStatement) statement;
        String tableName = stmt.getTableName();
        if (DatabaseManager.getCurrentDatabase() == null) {
            throw new Exception(ErrorHandler.noDatabaseSelected());
        }
        if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
            throw new Exception(ErrorHandler.tableNotFound(tableName));
        }
        List<ColumnDefinition> columns = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        if (columns == null) {
            throw new Exception(ErrorHandler.tableNotFound(tableName));
        }

        // 检查 SELECT 中的列
        List<String> selectColumns = stmt.getColumns();
        if (selectColumns != null) {
            for (String colName : selectColumns) {
                boolean found = false;
                for (ColumnDefinition colDef : columns) {
                    if (colDef.getName().equalsIgnoreCase(colName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new Exception(ErrorHandler.columnNotFound(colName));
                }
            }
        }

        // 新的：检查 Condition
        Condition cond = stmt.getCondition();
        checkConditionColumns(cond, columns);
        } else if (statement instanceof UpdateStatement) {
        UpdateStatement stmt = (UpdateStatement) statement;
        String tableName = stmt.getTableName();
        if (DatabaseManager.getCurrentDatabase() == null) {
            throw new Exception(ErrorHandler.noDatabaseSelected());
        }
        if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
            throw new Exception(ErrorHandler.tableNotFound(tableName));
        }
        // 读取表的 schema
        List<ColumnDefinition> columns = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        if (columns == null) {
            throw new Exception(ErrorHandler.tableNotFound(tableName));
        }
        // 构造一个用于快速查找的列名集合
        Set<String> colNames = new HashSet<>();
        for (ColumnDefinition col : columns) {
            colNames.add(col.getName());
        }
        // 检查每个 assignment 的列是否存在
        for (String colName : stmt.getAssignments().keySet()) {
            if (!colNames.contains(colName)) {
                throw new Exception(ErrorHandler.columnNotFound(colName));
            }
        }
        // 检查条件中使用的列是否存在（使用辅助方法）
        Condition cond = stmt.getCondition();
        checkConditionColumns(cond, columns);

        // 如果正在更新主键列，确保新值不会造成重复
        int pkIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isPrimaryKey()) {
                pkIndex = i;
                break;
            }
        }
        if (pkIndex != -1 && stmt.getAssignments().containsKey(columns.get(pkIndex).getName())) {
            String newPkValue = stmt.getAssignments().get(columns.get(pkIndex).getName());
            List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
            for (List<String> record : records) {
                if (record.size() > pkIndex && record.get(pkIndex).equals(newPkValue)) {
                    // 如果有 WHERE 条件，可能是同一行（可选处理），此处简化处理，直接报重复错误
                    throw new Exception(ErrorHandler.duplicatePrimaryKeyValue(newPkValue));
                }
            }
        }
        // 检查各 assignment 的类型匹配
        for (String colName : stmt.getAssignments().keySet()) {
            for (ColumnDefinition col : columns) {
                if (col.getName().equals(colName)) {
                    if (col.getType().toUpperCase().startsWith("INT")) {
                        String val = stmt.getAssignments().get(colName);
                        try {
                            Integer.parseInt(val);
                        } catch (NumberFormatException e) {
                            throw new Exception(ErrorHandler.typeMismatch(col.getName(), col.getType()));
                        }
                    }
                }
            }
        }
        }else if (statement instanceof DeleteStatement) {
        DeleteStatement stmt = (DeleteStatement) statement;
        String tableName = stmt.getTableName();
        if (DatabaseManager.getCurrentDatabase() == null) {
            throw new Exception(ErrorHandler.noDatabaseSelected());
        }
        if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
            throw new Exception(ErrorHandler.tableNotFound(tableName));
        }
        List<ColumnDefinition> columns = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        if (columns == null) {
            throw new Exception(ErrorHandler.tableNotFound(tableName));
        }
        // 新的：检查 condition 中用到的列
        Condition cond = stmt.getCondition();
        checkConditionColumns(cond, columns);
        }else if (statement instanceof JoinStatement) {
            JoinStatement stmt = (JoinStatement) statement;
            String table1 = stmt.getTable1();
            String table2 = stmt.getTable2();
            // 检查两个表是否存在
            if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), table1)) {
                throw new Exception(ErrorHandler.tableNotFound(table1));
            }
            if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), table2)) {
                throw new Exception(ErrorHandler.tableNotFound(table2));
            }
            // 检查各自表中是否包含指定的属性
            List<ColumnDefinition> schema1 = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), table1);
            List<ColumnDefinition> schema2 = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), table2);
            boolean foundAttr1 = false, foundAttr2 = false;
            for (ColumnDefinition col : schema1) {
                if (col.getName().equalsIgnoreCase(stmt.getAttribute1())) {
                    foundAttr1 = true;
                    break;
                }
            }
            for (ColumnDefinition col : schema2) {
                if (col.getName().equalsIgnoreCase(stmt.getAttribute2())) {
                    foundAttr2 = true;
                    break;
                }
            }
            if (!foundAttr1) {
                throw new Exception(ErrorHandler.columnNotFound(stmt.getAttribute1()));
            }
            if (!foundAttr2) {
                throw new Exception(ErrorHandler.columnNotFound(stmt.getAttribute2()));
            }
        } else if (statement instanceof AlterTableStatement) {
            AlterTableStatement stmt = (AlterTableStatement) statement;
            String tableName = stmt.getTableName();
            if (DatabaseManager.getCurrentDatabase() == null) {
                throw new Exception(ErrorHandler.noDatabaseSelected());
            }
            if (!StorageManager.tableExists(DatabaseManager.getCurrentDatabase(), tableName)) {
                throw new Exception(ErrorHandler.tableNotFound(tableName));
            }
            List<ColumnDefinition> columns = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
            String attrName = stmt.getAttributeName();
            if (stmt.getAlterationType().equals("ADD")) {
                // 检查属性是否已存在
                for (ColumnDefinition col : columns) {
                    if (col.getName().equalsIgnoreCase(attrName)) {
                        throw new Exception(ErrorHandler.duplicateColumnName(attrName));
                    }
                }
            } else if (stmt.getAlterationType().equals("DROP")) {
                // 检查属性是否存在
                boolean found = false;
                for (ColumnDefinition col : columns) {
                    if (col.getName().equalsIgnoreCase(attrName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new Exception(ErrorHandler.columnNotFound(attrName));
                }
            }
        }
    }

    private void checkConditionColumns(Condition cond, List<ColumnDefinition> columns) throws Exception {
        if (cond == null) {
            return; // 没有 WHERE 条件，直接返回
        }
        if (cond instanceof SimpleCondition) {
            SimpleCondition sc = (SimpleCondition) cond;
            String attr = sc.getAttribute();
            // 检查 attr 是否在表的 schema 中
            boolean found = false;
            for (ColumnDefinition col : columns) {
                if (col.getName().equalsIgnoreCase(attr)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new Exception(ErrorHandler.columnNotFound(attr));
            }
        } else if (cond instanceof CompoundCondition) {
            CompoundCondition cc = (CompoundCondition) cond;
            // 递归检查左、右条件
            checkConditionColumns(cc.getLeft(), columns);
            checkConditionColumns(cc.getRight(), columns);
        }
    }


}
