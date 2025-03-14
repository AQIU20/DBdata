package edu.uob;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryExecutor {

    public static String executeSelect(SelectStatement stmt) {
        String tableName = stmt.getTableName();
        List<String> selectColumns = stmt.getColumns();
        Condition condition = stmt.getCondition();

        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> resultRecords = new ArrayList<>();

        for (List<String> record : records) {
            if (condition == null || evaluateCondition(condition, record, schema)) {
                resultRecords.add(record);
            }
        }

        if (resultRecords.isEmpty()) {
            return "Empty set.";
        }

        List<Integer> colIndexes = getSelectedColumnIndexes(selectColumns, schema);
        return buildSelectOutput(schema, resultRecords, colIndexes);
    }

    private static List<Integer> getSelectedColumnIndexes(List<String> selectColumns, List<ColumnDefinition> schema) {
        List<Integer> indexes = new ArrayList<>();
        if (selectColumns == null || selectColumns.isEmpty()) {
            for (int i = 0; i < schema.size(); i++) {
                indexes.add(i);
            }
        } else {
            for (String colName : selectColumns) {
                for (int i = 0; i < schema.size(); i++) {
                    if (schema.get(i).getName().equalsIgnoreCase(colName)) {
                        indexes.add(i);
                    }
                }
            }
        }
        return indexes;
    }

    private static String buildSelectOutput(List<ColumnDefinition> schema, List<List<String>> records, List<Integer> indexes) {
        StringBuilder output = new StringBuilder();
        if (indexes.size() > 1 || indexes.size() == 1) {
            appendHeader(schema, indexes, output);
        }
        appendRecords(records, indexes, output);
        return output.toString().trim();
    }

    private static void appendHeader(List<ColumnDefinition> schema, List<Integer> indexes, StringBuilder output) {
        for (int i = 0; i < indexes.size(); i++) {
            output.append(schema.get(indexes.get(i)).getName());
            if (i < indexes.size() - 1) {
                output.append(" | ");
            }
        }
        output.append("\n");
    }

    private static void appendRecords(List<List<String>> records, List<Integer> indexes, StringBuilder output) {
        for (int i = 0; i < records.size(); i++) {
            List<String> record = records.get(i);
            for (int j = 0; j < indexes.size(); j++) {
                int idx = indexes.get(j);
                String value = idx < record.size() ? record.get(idx) : "";
                output.append(value);
                if (j < indexes.size() - 1) {
                    output.append(" | ");
                }
            }
            if (i < records.size() - 1) {
                output.append("\n");
            }
        }
    }

    public static String executeUpdate(UpdateStatement stmt) {
        String tableName = stmt.getTableName();
        Condition condition = stmt.getCondition();
        Map<String, String> assignments = stmt.getAssignments();

        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
        Map<String, Integer> colIndexMap = buildColumnIndexMap(schema);

        int updateCount = processUpdateRecords(records, condition, schema, assignments, colIndexMap);
        boolean success = StorageManager.writeTableRecords(DatabaseManager.getCurrentDatabase(), tableName, schema, records);

        if (!success) {
            return ErrorHandler.generalError("Failed to update rows.");
        }
        return "";
    }

    private static Map<String, Integer> buildColumnIndexMap(List<ColumnDefinition> schema) {
        Map<String, Integer> map = new java.util.HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            map.put(schema.get(i).getName(), i);
        }
        return map;
    }

    private static int processUpdateRecords(List<List<String>> records, Condition condition,
                                            List<ColumnDefinition> schema, Map<String, String> assignments,
                                            Map<String, Integer> colIndexMap) {
        int count = 0;
        for (List<String> record : records) {
            if (condition == null || evaluateCondition(condition, record, schema)) {
                updateRecord(record, assignments, colIndexMap);
                count++;
            }
        }
        return count;
    }

    private static void updateRecord(List<String> record, Map<String, String> assignments, Map<String, Integer> colIndexMap) {
        for (Map.Entry<String, String> entry : assignments.entrySet()) {
            String colName = entry.getKey();
            String value = entry.getValue();
            Integer idx = colIndexMap.get(colName);
            if (idx != null) {
                ensureCapacity(record, idx);
                record.set(idx, value);
            }
        }
    }

    private static void ensureCapacity(List<String> list, int index) {
        while (list.size() <= index) {
            list.add("");
        }
    }

    public static String executeDelete(DeleteStatement stmt) {
        String tableName = stmt.getTableName();
        Condition condition = stmt.getCondition();

        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        int deleteCount = filterRecords(records, condition, schema);
        boolean success = StorageManager.writeTableRecords(DatabaseManager.getCurrentDatabase(), tableName, schema, records);

        if (!success) {
            return ErrorHandler.generalError("Failed to delete rows.");
        }
        return "";
    }

    private static int filterRecords(List<List<String>> records, Condition condition, List<ColumnDefinition> schema) {
        int count = 0;
        Iterator<List<String>> it = records.iterator();
        while (it.hasNext()) {
            List<String> record = it.next();
            if (condition == null || evaluateCondition(condition, record, schema)) {
                it.remove();
                count++;
            }
        }
        return count;
    }

    public static String executeInsert(InsertStatement stmt) {
        String tableName = stmt.getTableName();
        List<String> userValues = stmt.getValues();

        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        if (schema == null) {
            return ErrorHandler.tableNotFound(tableName);
        }

        if (userValues.size() != schema.size() - 1) {
            logColumnMismatch(schema.size() - 1, userValues.size());
            return ErrorHandler.columnCountMismatch();
        }

        processUserValues(userValues);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);
        int newId = calculateNewId(records);

        List<String> newRow = buildNewRow(newId, userValues);
        boolean success = StorageManager.insertRow(DatabaseManager.getCurrentDatabase(), tableName, String.join("\t", newRow));

        return success ? ""  : ErrorHandler.generalError("Failed to insert row.");
    }

    private static void logColumnMismatch(int expected, int actual) {
        System.err.println("[ERROR] Expected columns: " + expected);
        System.err.println("[ERROR] Provided values: " + actual);
        System.err.flush();
    }

    private static void processUserValues(List<String> values) {
        for (int i = 0; i < values.size(); i++) {
            String val = values.get(i);
            if (val.length() >= 2 && val.startsWith("'") && val.endsWith("'")) {
                values.set(i, val.substring(1, val.length() - 1));
            }
        }
    }

    private static int calculateNewId(List<List<String>> records) {
        int max = 0;
        for (List<String> record : records) {
            if (!record.isEmpty()) {
                try {
                    int id = Integer.parseInt(record.get(0));
                    if (id > max) max = id;
                } catch (NumberFormatException e) {
                    // 忽略无效ID
                }
            }
        }
        return max + 1;
    }

    private static List<String> buildNewRow(int id, List<String> values) {
        List<String> row = new ArrayList<>();
        row.add(String.valueOf(id));
        row.addAll(values);
        return row;
    }

    private static boolean evaluateCondition(Condition cond, List<String> record, List<ColumnDefinition> schema) {
        if (cond == null) return true;
        if (cond instanceof SimpleCondition) {
            return evaluateSimpleCondition((SimpleCondition) cond, record, schema);
        } else if (cond instanceof CompoundCondition) {
            CompoundCondition cc = (CompoundCondition) cond;
            boolean left = evaluateCondition(cc.getLeft(), record, schema);
            boolean right = evaluateCondition(cc.getRight(), record, schema);
            if ("AND".equals(cc.getOperator())) {
                return left && right;
            } else if ("OR".equals(cc.getOperator())) {
                return left || right;
            }
        }
        return false;
    }

    private static boolean evaluateSimpleCondition(SimpleCondition cond, List<String> record, List<ColumnDefinition> schema) {
        String attr = cond.getAttribute();
        String comp = cond.getComparator();
        String value = processConditionValue(cond.getValue());

        int attrIndex = findAttributeIndex(attr, schema);
        if (attrIndex == -1 || attrIndex >= record.size()) {
            return false;
        }

        String recordValue = processRecordValue(record.get(attrIndex));
        return compareValues(recordValue, value, comp);
    }

    private static String processConditionValue(String value) {
        if (value != null && value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static int findAttributeIndex(String name, List<ColumnDefinition> schema) {
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    private static String processRecordValue(String value) {
        if (value != null && value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static boolean compareValues(String val1, String val2, String comparator) {
        if ("=".equals(comparator) || "==".equals(comparator)) {
            return val1.equalsIgnoreCase(val2);
        } else if ("!=".equals(comparator)) {
            return !val1.equalsIgnoreCase(val2);
        } else if (">".equals(comparator)) {
            return compareAsNumber(val1, val2) > 0;
        } else if ("<".equals(comparator)) {
            return compareAsNumber(val1, val2) < 0;
        } else if (">=".equals(comparator)) {
            return compareAsNumber(val1, val2) >= 0;
        } else if ("<=".equals(comparator)) {
            return compareAsNumber(val1, val2) <= 0;
        } else if ("LIKE".equals(comparator)) {
            return val1.toLowerCase().contains(val2.toLowerCase());
        }
        return false;
    }

    private static int compareAsNumber(String a, String b) {
        try {
            double numA = Double.parseDouble(a);
            double numB = Double.parseDouble(b);
            return Double.compare(numA, numB);
        } catch (NumberFormatException e) {
            return a.compareToIgnoreCase(b);
        }
    }

    public static String executeJoin(JoinStatement stmt) {
        String table1 = stmt.getTable1();
        String table2 = stmt.getTable2();
        String matchAttr1 = stmt.getAttribute1();
        String matchAttr2 = stmt.getAttribute2();

        // 读取表结构和数据
        List<ColumnDefinition> schema1 = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), table1);
        List<ColumnDefinition> schema2 = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), table2);
        List<List<String>> data1 = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), table1);
        List<List<String>> data2 = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), table2);

        // 查找匹配列索引
        int matchIdx1 = findAttributeIndex(matchAttr1, schema1);
        int matchIdx2 = findAttributeIndex(matchAttr2, schema2);

        // 执行JOIN核心逻辑
        List<List<String>> joinedData = new ArrayList<>();
        int newId = 1;
        for (List<String> row1 : data1) {
            for (List<String> row2 : data2) {
                if (isJoinMatch(row1, matchIdx1, row2, matchIdx2)) {
                    // 生成新行并添加ID
                    List<String> newRow = buildJoinedRow(table1, table2, row1, row2,
                            schema1, schema2, matchIdx1, matchIdx2);
                    newRow.add(0, String.valueOf(newId++));
                    joinedData.add(newRow);
                }
            }
        }

        // 构建输出
        return buildJoinOutput(table1, table2, schema1, schema2, matchIdx1, matchIdx2, joinedData);
    }

    private static List<List<String>> performJoin(List<List<String>> data1, List<List<String>> data2,
                                                  int idx1, int idx2,
                                                  List<ColumnDefinition> schema1, List<ColumnDefinition> schema2) {
        List<List<String>> results = new ArrayList<>();
        for (List<String> row1 : data1) {
            for (List<String> row2 : data2) {
                if (isJoinMatch(row1, idx1, row2, idx2)) {
                    // 合并所有列（包括重复列）
                    results.add(mergeRows(row1, row2, schema1, schema2));
                }
            }
        }
        return results;
    }

    private static boolean isJoinMatch(List<String> row1, int idx1,
                                       List<String> row2, int idx2) {
        if (row1.size() <= idx1 || row2.size() <= idx2) return false;
        return row1.get(idx1).equals(row2.get(idx2));
    }

    private static List<String> mergeRows(List<String> row1, List<String> row2,
                                          List<ColumnDefinition> schema1, List<ColumnDefinition> schema2) {
        List<String> merged = new ArrayList<>(row1);
        // 添加第二个表的所有列
        merged.addAll(row2);
        return merged;
    }


    private static boolean isColumnExist(String name, List<ColumnDefinition> schema) {
        for (ColumnDefinition col : schema) {
            if (col.getName().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> buildJoinedRow(String table1, String table2,
                                               List<String> row1, List<String> row2,
                                               List<ColumnDefinition> schema1,
                                               List<ColumnDefinition> schema2,
                                               int excludeIdx1, int excludeIdx2) {
        List<String> newRow = new ArrayList<>();

        // 添加表1字段（排除ID和匹配列）
        for (int i=0; i<schema1.size(); i++) {
            if (i != 0 && i != excludeIdx1) { // 跳过ID列和匹配列
                newRow.add(row1.get(i));
            }
        }

        // 添加表2字段（排除ID和匹配列）
        for (int i=0; i<schema2.size(); i++) {
            if (i != 0 && i != excludeIdx2) { // 跳过ID列和匹配列
                newRow.add(row2.get(i));
            }
        }

        return newRow;
    }

    private static String buildJoinOutput(String table1, String table2,
                                          List<ColumnDefinition> schema1,
                                          List<ColumnDefinition> schema2,
                                          int excludeIdx1, int excludeIdx2,
                                          List<List<String>> data) {
        StringBuilder output = new StringBuilder("id | ");

        // 生成表头
        List<String> headers = new ArrayList<>();
        for (int i=0; i<schema1.size(); i++) {
            if (i != 0 && i != excludeIdx1) {
                headers.add(table1 + "." + schema1.get(i).getName());
            }
        }
        for (int i=0; i<schema2.size(); i++) {
            if (i != 0 && i != excludeIdx2) {
                headers.add(table2 + "." + schema2.get(i).getName());
            }
        }
        output.append(String.join(" | ", headers)).append("\n");

        // 添加数据行
        for (List<String> row : data) {
            output.append(String.join(" | ", row)).append("\n");
        }

        return output.toString().trim();
    }

    private static void appendJoinHeader(String table1, String table2,
                                         List<ColumnDefinition> schema1, List<ColumnDefinition> schema2,
                                         StringBuilder output) {
        List<String> headers = new ArrayList<>();
        // 添加第一个表的列名，带表名前缀
        for (ColumnDefinition col : schema1) {
            headers.add(table1 + "." + col.getName());
        }
        // 添加第二个表的列名，带表名前缀
        for (ColumnDefinition col : schema2) {
            headers.add(table2 + "." + col.getName());
        }
        output.append(String.join(" | ", headers));
        output.append("\n");
    }

    private static void appendJoinData(List<List<String>> data, StringBuilder output) {
        for (int i = 0; i < data.size(); i++) {
            List<String> row = data.get(i);
            output.append(String.join(" | ", row));
            if (i < data.size() - 1) {
                output.append("\n");
            }
        }
    }

    public static String executeAlter(AlterTableStatement stmt) {
        String tableName = stmt.getTableName();
        String operation = stmt.getAlterationType();
        String columnName = stmt.getAttributeName();

        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        if ("ADD".equals(operation)) {
            handleAddColumn(schema, records, columnName);
        } else if ("DROP".equals(operation)) {
            handleDropColumn(schema, records, columnName);
        }

        boolean success = StorageManager.writeTableRecords(DatabaseManager.getCurrentDatabase(), tableName, schema, records);
        return success ? "" : ErrorHandler.generalError("Failed to alter table.");
    }

    private static void handleAddColumn(List<ColumnDefinition> schema, List<List<String>> records, String colName) {
        schema.add(new ColumnDefinition(colName, "TEXT", false));
        for (List<String> record : records) {
            record.add("");
        }
    }

    private static void handleDropColumn(List<ColumnDefinition> schema, List<List<String>> records, String colName) {
        int colIndex = -1;
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).getName().equalsIgnoreCase(colName)) {
                colIndex = i;
                break;
            }
        }
        if (colIndex != -1) {
            schema.remove(colIndex);
            for (List<String> record : records) {
                if (record.size() > colIndex) {
                    record.remove(colIndex);
                }
            }
        }
    }
}