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
        Condition condition = stmt.getCondition();      // 新增：从 statement 中获取 Condition

        // 读取表的 schema 和记录
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        // 根据 condition 过滤记录
        List<List<String>> resultRecords = new ArrayList<>();
        for (List<String> record : records) {
            if (condition == null || evaluateCondition(condition, record, schema)) {
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
     * Execute an UPDATE statement and return result message.
     */
    public static String executeUpdate(UpdateStatement stmt) {
        String tableName = stmt.getTableName();
        Condition condition = stmt.getCondition();      // 新增
        Map<String, String> assignments = stmt.getAssignments();

        // Read current records
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        // Build column index map
        Map<String, Integer> colIndexMap = new java.util.HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            colIndexMap.put(schema.get(i).getName(), i);
        }

        int updateCount = 0;
        for (List<String> record : records) {
            // 只更新符合 condition 的记录
            if (condition == null || evaluateCondition(condition, record, schema)) {
                // 更新指定列
                for (Map.Entry<String, String> assign : assignments.entrySet()) {
                    String colName = assign.getKey();
                    String newValue = assign.getValue();
                    Integer idx = colIndexMap.get(colName);
                    if (idx != null) {
                        // 如果 record 长度不足则填充
                        while (record.size() <= idx) {
                            record.add("");
                        }
                        record.set(idx, newValue);
                    }
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
        Condition condition = stmt.getCondition();      // 新增

        // Read current records
        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        // 遍历记录，如果符合 condition 则删除
        Iterator<List<String>> iterator = records.iterator();
        int deleteCount = 0;
        while (iterator.hasNext()) {
            List<String> record = iterator.next();
            if (condition == null || evaluateCondition(condition, record, schema)) {
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
    private static boolean evaluateCondition(Condition cond, List<String> record, List<ColumnDefinition> schema) {
        if (cond == null) {
            // 没有条件，默认返回true
            return true;
        }
        if (cond instanceof SimpleCondition) {
            return evaluateSimpleCondition((SimpleCondition) cond, record, schema);
        } else if (cond instanceof CompoundCondition) {
            CompoundCondition cc = (CompoundCondition) cond;
            boolean leftResult = evaluateCondition(cc.getLeft(), record, schema);
            boolean rightResult = evaluateCondition(cc.getRight(), record, schema);
            if (cc.getOperator().equals("AND")) {
                return leftResult && rightResult;
            } else if (cc.getOperator().equals("OR")) {
                return leftResult || rightResult;
            }
        }
        // 如果出现未知类型，或者 operator 非 AND/OR，默认 false
        return false;
    }

    /**
     * 处理简单条件：attribute comparator value
     */
    private static boolean evaluateSimpleCondition(SimpleCondition cond, List<String> record, List<ColumnDefinition> schema) {
        String attrName = cond.getAttribute();
        String comparator = cond.getComparator();
        String rawValue = cond.getValue();

        // 去除引号
        if (rawValue != null && rawValue.length() >= 2 &&
                rawValue.startsWith("'") && rawValue.endsWith("'")) {
            rawValue = rawValue.substring(1, rawValue.length() - 1);
        }

        // 找到属性在 schema 中的索引
        int attrIndex = -1;
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).getName().equalsIgnoreCase(attrName)) {
                attrIndex = i;
                break;
            }
        }
        if (attrIndex == -1) {
            // 属性不存在，按需处理：要么抛出异常，要么直接返回 false
            return false;
        }
        if (attrIndex >= record.size()) {
            // 记录中没有这个列，认为不匹配
            return false;
        }

        String recordValue = record.get(attrIndex);

        // 下面根据 comparator 做简单的示例比较，可自行扩展
        // 忽略大小写
        switch (comparator) {
            case "=":
            case "==":
                return recordValue.equalsIgnoreCase(rawValue);
            case "!=":
                return !recordValue.equalsIgnoreCase(rawValue);
            case ">":
                // 尝试转为数字比较，若失败则按字符串 compareTo
                return compareAsNumberOrString(recordValue, rawValue) > 0;
            case "<":
                return compareAsNumberOrString(recordValue, rawValue) < 0;
            case ">=":
                return compareAsNumberOrString(recordValue, rawValue) >= 0;
            case "<=":
                return compareAsNumberOrString(recordValue, rawValue) <= 0;
            case "LIKE":
                // 简化版，忽略大小写
                return recordValue.toLowerCase().contains(rawValue.toLowerCase());
            default:
                // 不支持的 comparator
                return false;
        }
    }

    /**
     * 辅助方法：先尝试将字符串转为 Double，若无法转换则按字符串 compareTo
     * 返回：正数 表示 recordValue > rawValue，0 表示相等，负数表示 recordValue < rawValue
     */
    private static int compareAsNumberOrString(String recordValue, String rawValue) {
        try {
            double rv = Double.parseDouble(recordValue);
            double rv2 = Double.parseDouble(rawValue);
            return Double.compare(rv, rv2);
        } catch (NumberFormatException e) {
            // 如果无法转为数字，则使用不区分大小写的字符串比较
            return recordValue.compareToIgnoreCase(rawValue);
        }
    }

    public static String executeJoin(JoinStatement stmt) {
        String table1 = stmt.getTable1();
        String table2 = stmt.getTable2();
        String attr1 = stmt.getAttribute1();
        String attr2 = stmt.getAttribute2();

        // 读取两个表的 schema 和记录
        List<ColumnDefinition> schema1 = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), table1);
        List<ColumnDefinition> schema2 = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), table2);
        List<List<String>> records1 = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), table1);
        List<List<String>> records2 = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), table2);

        // 找出 join 属性在各自表中的索引
        int joinIndex1 = -1, joinIndex2 = -1;
        for (int i = 0; i < schema1.size(); i++) {
            if (schema1.get(i).getName().equalsIgnoreCase(attr1)) {
                joinIndex1 = i;
                break;
            }
        }
        for (int i = 0; i < schema2.size(); i++) {
            if (schema2.get(i).getName().equalsIgnoreCase(attr2)) {
                joinIndex2 = i;
                break;
            }
        }
        if (joinIndex1 == -1 || joinIndex2 == -1) {
            return ErrorHandler.syntaxError();
        }

        // 内连接：对 table1 中的每条记录，找出 table2 中 join 列匹配的记录
        List<List<String>> joinResults = new ArrayList<>();
        for (List<String> r1 : records1) {
            for (List<String> r2 : records2) {
                if (r1.size() > joinIndex1 && r2.size() > joinIndex2 &&
                        r1.get(joinIndex1).equalsIgnoreCase(r2.get(joinIndex2))) {

                    // 合并两行，但对于 table2 的列，如果其名称在 table1 中已存在，则跳过（避免重复）
                    List<String> joined = new ArrayList<>();
                    // 添加 table1 的所有列
                    joined.addAll(r1);

                    // 对于 table2，每个列检查是否在 table1 schema 中存在（忽略大小写）
                    for (int j = 0; j < schema2.size(); j++) {
                        String colName2 = schema2.get(j).getName();
                        boolean duplicate = false;
                        for (ColumnDefinition col1 : schema1) {
                            if (col1.getName().equalsIgnoreCase(colName2)) {
                                duplicate = true;
                                break;
                            }
                        }
                        if (!duplicate) {
                            if (j < r2.size()) {
                                joined.add(r2.get(j));
                            } else {
                                joined.add("");
                            }
                        }
                    }
                    joinResults.add(joined);
                }
            }
        }

        // 构造输出字符串
        StringBuilder output = new StringBuilder();
        // 构造 header：table1 的所有列，table2 中仅包含非重复的列
        for (int i = 0; i < schema1.size(); i++) {
            output.append(schema1.get(i).getName());
            if (i < schema1.size() - 1) {
                output.append(" | ");
            }
        }
        for (int j = 0; j < schema2.size(); j++) {
            String colName2 = schema2.get(j).getName();
            boolean duplicate = false;
            for (ColumnDefinition col1 : schema1) {
                if (col1.getName().equalsIgnoreCase(colName2)) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                output.append(" | ").append(colName2);
            }
        }
        output.append("\n");

        // 构造数据行
        if (joinResults.isEmpty()) {
            output.append("Empty set.");
        } else {
            for (List<String> row : joinResults) {
                for (int i = 0; i < row.size(); i++) {
                    output.append(row.get(i));
                    if (i < row.size() - 1) {
                        output.append(" | ");
                    }
                }
                output.append("\n");
            }
        }
        return output.toString().trim();
    }

    public static String executeAlter(AlterTableStatement stmt) {
        String tableName = stmt.getTableName();
        String alterationType = stmt.getAlterationType();
        String attributeName = stmt.getAttributeName();

        List<ColumnDefinition> schema = StorageManager.readTableSchema(DatabaseManager.getCurrentDatabase(), tableName);
        List<List<String>> records = StorageManager.readTableRecords(DatabaseManager.getCurrentDatabase(), tableName);

        if (alterationType.equals("ADD")) {
            // 添加新属性到 schema
            schema.add(new ColumnDefinition(attributeName, "TEXT", false));
            // 对每条记录，追加空字符串
            for (List<String> record : records) {
                record.add("");
            }
        } else if (alterationType.equals("DROP")) {
            // 找到要删除的属性在 schema 中的索引
            int dropIndex = -1;
            for (int i = 0; i < schema.size(); i++) {
                if (schema.get(i).getName().equalsIgnoreCase(attributeName)) {
                    dropIndex = i;
                    break;
                }
            }
            if (dropIndex == -1) {
                return ErrorHandler.columnNotFound(attributeName);
            }
            schema.remove(dropIndex);
            // 对每条记录，删除对应位置的值
            for (List<String> record : records) {
                if (record.size() > dropIndex) {
                    record.remove(dropIndex);
                }
            }
        }
        boolean success = StorageManager.writeTableRecords(DatabaseManager.getCurrentDatabase(), tableName, schema, records);
        if (!success) {
            return ErrorHandler.generalError("Failed to alter table.");
        }
        return "Table altered successfully.";
    }


}
