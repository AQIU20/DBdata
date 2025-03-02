package edu.uob;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryExecutor {
    /**
     * Execute a SELECT statement and return the result as a string.
     */
    public static String executeSelect(SelectStatement stmt) {
        String tableName = stmt.getTableName();
        List<String> selectColumns = stmt.getColumns(); // null means all columns
        String whereCol = stmt.getWhereColumn();
        String whereVal = stmt.getWhereValue();

        // 如果 whereVal 两侧有单引号，则去除它们
        if (whereVal != null && whereVal.length() >= 2 &&
                whereVal.startsWith("'") && whereVal.endsWith("'")) {
            whereVal = whereVal.substring(1, whereVal.length() - 1);
        }


        // 读取表的 schema 和记录
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        // 如果存在 WHERE 子句，先找出对应的列索引（使用 equalsIgnoreCase）
        int whereIndex = -1;
        if (whereCol != null) {
            for (int i = 0; i < schema.size(); i++) {
                if (schema.get(i).getName().equalsIgnoreCase(whereCol)) {
                    whereIndex = i;
                    break;
                }
            }
        }

        // 过滤记录：使用不区分大小写比较字符串
        List<List<String>> resultRecords = new ArrayList<>();
        for (List<String> record : records) {
            if (whereIndex != -1) {
                if (record.size() > whereIndex && record.get(whereIndex).equalsIgnoreCase(whereVal)) {
                    resultRecords.add(record);
                }
            } else {
                // 没有 WHERE 子句，包含所有记录
                resultRecords.add(record);
            }
        }

        if (resultRecords.isEmpty()) {
            return "Empty set.";
        }

        // 确定要输出的列索引
        List<Integer> colIndexes;
        if (selectColumns == null || selectColumns.isEmpty()) {
            colIndexes = new ArrayList<>();
            for (int i = 0; i < schema.size(); i++) {
                colIndexes.add(i);
            }
        } else {
            colIndexes = new ArrayList<>();
            for (String colName : selectColumns) {
                for (int i = 0; i < schema.size(); i++) {
                    if (schema.get(i).getName().equalsIgnoreCase(colName)) {
                        colIndexes.add(i);
                    }
                }
            }
        }

        // 构造输出字符串
        StringBuilder output = new StringBuilder();
        // 输出标题行（如果有多个列或是全列查询）
        if (colIndexes.size() > 1 || (selectColumns == null && colIndexes.size() == 1)) {
            for (int j = 0; j < colIndexes.size(); j++) {
                output.append(schema.get(colIndexes.get(j)).getName());
                if (j < colIndexes.size() - 1) {
                    output.append(" | ");
                }
            }
            output.append("\n");
        }

        // 输出数据行
        for (int r = 0; r < resultRecords.size(); r++) {
            List<String> record = resultRecords.get(r);
            for (int j = 0; j < colIndexes.size(); j++) {
                int idx = colIndexes.get(j);
                String value = "";
                if (idx < record.size()) {
                    value = record.get(idx);
                }
                output.append(value);
                if (j < colIndexes.size() - 1) {
                    output.append(" | ");
                }
            }
            if (r < resultRecords.size() - 1) {
                output.append("\n");
            }
        }
        return output.toString();
    }



    /**
     * Execute an INSERT statement and return result message.
     * 修改部分：在插入时自动在首列生成自增长 id，
     * 用户仅需提供除 id 外的字段数据。
     */
    public static String executeInsert(InsertStatement stmt) {
        String tableName = stmt.getTableName();
        List<String> userValues = stmt.getValues();

        // 读取表 schema，自动添加的 id 列已在第一位
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        if (schema == null) {
            return ErrorHandler.tableNotFound(tableName);
        }
        // 用户提供的值数量应为 schema.size()-1
        if (userValues.size() != schema.size() - 1) {
            System.err.println("[ERROR] Expected columns (excluding auto id): " + (schema.size() - 1));
            System.err.println("[ERROR] Provided values: " + userValues.size());
            System.err.flush();
            return ErrorHandler.columnCountMismatch();
        }

        // 计算新的 id：遍历现有记录，取最大 id，然后新 id = max + 1；若无记录，则新 id 为 1
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
        int maxId = 0;
        for (List<String> record : records) {
            if (!record.isEmpty()) {
                try {
                    int id = Integer.parseInt(record.get(0));
                    if (id > maxId) {
                        maxId = id;
                    }
                } catch (NumberFormatException e) {
                    // 如果无法解析 id，则忽略该记录
                }
            }
        }
        int newId = maxId + 1;

        // 构造新行：先添加自动生成的 id，再追加用户提供的数据
        List<String> newRow = new ArrayList<>();
        newRow.add(String.valueOf(newId));
        newRow.addAll(userValues);

        // 将新行转换为以逗号分隔的字符串
        String row = String.join(", ", newRow);
        boolean success = StorageManager.insertRow(DatabaseManager.getCurrentDatabase(), tableName, row);
        if (!success) {
            return ErrorHandler.generalError("Failed to insert row.");
        }
        return "1 row inserted.";
    }

    /**
     * Execute an UPDATE statement and return result message.
     */
    public static String executeUpdate(UpdateStatement stmt) {
        String tableName = stmt.getTableName();
        String whereCol = stmt.getWhereColumn();
        String whereVal = stmt.getWhereValue();
        Map<String, String> assignments = stmt.getAssignments();
        // Read current records
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
        // Build column index map
        Map<String, Integer> colIndexMap = new java.util.HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            colIndexMap.put(schema.get(i).getName(), i);
        }
        Integer whereIndex = null;
        if (whereCol != null) {
            whereIndex = colIndexMap.get(whereCol);
        }
        // Apply updates
        int updateCount = 0;
        for (List<String> record : records) {
            if (whereIndex == null || (whereIndex < record.size() && record.get(whereIndex).equals(whereVal))) {
                // Update指定列的值
                for (Map.Entry<String, String> assign : assignments.entrySet()) {
                    String colName = assign.getKey();
                    String newValue = assign.getValue();
                    int idx = colIndexMap.get(colName);
                    if (idx >= record.size()) {
                        while (record.size() <= idx) {
                            record.add("");
                        }
                    }
                    record.set(idx, newValue);
                }
                updateCount++;
            }
        }
        // 写回所有记录
        boolean success = StorageManager.writeTableRecords(DatabaseManager.getCurrentDatabase(), tableName, schema, records);
        if (!success) {
            return ErrorHandler.generalError("Failed to update rows.");
        }
        return updateCount + " row(s) updated.";
    }

    /**
     * Execute a DELETE statement and return result message.
     */
    public static String executeDelete(DeleteStatement stmt) {
        String tableName = stmt.getTableName();
        String whereCol = stmt.getWhereColumn();
        String whereVal = stmt.getWhereValue();
        // Read current records
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
        // Determine index for where column if any
        int whereIndex = -1;
        if (whereCol != null) {
            for (int i = 0; i < schema.size(); i++) {
                if (schema.get(i).getName().equals(whereCol)) {
                    whereIndex = i;
                    break;
                }
            }
        }
        // Filter out records matching where clause
        Iterator<List<String>> iterator = records.iterator();
        int deleteCount = 0;
        while (iterator.hasNext()) {
            List<String> record = iterator.next();
            if (whereIndex == -1 || (whereIndex < record.size() && record.get(whereIndex).equals(whereVal))) {
                iterator.remove();
                deleteCount++;
            }
        }
        // 写回剩余记录
        boolean success = StorageManager.writeTableRecords(DatabaseManager.getCurrentDatabase(), tableName, schema, records);
        if (!success) {
            return ErrorHandler.generalError("Failed to delete rows.");
        }
        return deleteCount + " row(s) deleted.";
    }
}
