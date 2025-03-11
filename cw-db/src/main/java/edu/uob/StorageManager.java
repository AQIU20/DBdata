package edu.uob;

import edu.uob.ColumnDefinition;
import edu.uob.DBServer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件存储
 */
public class StorageManager {
    private static final String BASE_PATH = "databases"; // 数据库存储在 databases 文件夹下

    public static boolean databaseExists(String dbName) {
        File dbDir = new File(BASE_PATH, dbName);
        return dbDir.exists() && dbDir.isDirectory();
    }

    public static boolean createDatabase(String dbName) {
        File dbDir = new File(BASE_PATH, dbName);
        if (dbDir.exists()) {
            return false;
        }
        return dbDir.mkdir();
    }

    public static boolean deleteDatabase(String dbName) {
        File dbDir = new File(BASE_PATH, dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            return false;
        }
        // 删除目录内所有文件
        File[] files = dbDir.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
        // 删除目录本身
        return dbDir.delete();
    }

    public static boolean tableExists(String dbName, String tableName) {
        if (dbName == null) return false;
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        return tableFile.exists() && tableFile.isFile();
    }

    /**
     * 修改后：在创建表时自动在 schema 前添加 id 列，写入的 schema 为 "id\t" + (将逗号替换为制表符的原 schema)。
     * 注意：这里只写入列名，后续解析时会默认给 id 列赋予 INT 类型和主键属性。
     */
    public static boolean createTable(String dbName, String tableName, String schemaLine) {
        if (dbName == null) return false;
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (tableFile.exists()) {
            return false;
        }
        // 将 schemaLine 中的逗号及其周围的空白替换为单个制表符
        schemaLine = schemaLine.replaceAll("\\s*,\\s*", "\t");
        // 在 schema 前加上 "id" 列，使用制表符分隔
        String newSchemaLine = "id\t" + schemaLine;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
            writer.write(newSchemaLine);
            writer.newLine();
        } catch (IOException e) {
            return false;
        }
        return true;
    }


    public static boolean deleteTable(String dbName, String tableName) {
        if (dbName == null) return false;
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) {
            return false;
        }
        return tableFile.delete();
    }

    /**
     * 修改后的 readTableSchema 方法：
     * 解析 schema 行时，如果列定义只有单个单词，则：
     * - 如果列名为 "id"，默认类型为 INT 且标记为主键；
     * - 否则默认类型为 TEXT，非主键。
     * 如果列定义包含多个单词，则按原有逻辑解析。
     * 使用制表符分隔各列定义。
     */
    public static List<ColumnDefinition> readTableSchema(String dbName, String tableName) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) {
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String schemaLine = reader.readLine();
            if (schemaLine == null) {
                return null;
            }
            List<ColumnDefinition> columns = new ArrayList<>();
            // 使用制表符拆分
            String[] colDefs = schemaLine.split("\t");
            for (String colDef : colDefs) {
                colDef = colDef.trim();
                if (colDef.isEmpty()) continue;
                // 检查是否包含 "PRIMARY KEY" 字符串
                boolean pk = colDef.toUpperCase().contains("PRIMARY KEY");
                // 移除 PRIMARY KEY 部分（如果存在）
                String cleanDef = colDef.replaceAll("(?i)PRIMARY KEY", "").trim();
                String[] parts = cleanDef.split("\\s+");
                if (parts.length == 1) {
                    // 只有单个单词：如果为 id，则默认类型为 INT 且主键；否则默认类型 TEXT
                    String name = parts[0];
                    String type;
                    boolean isPrimary;
                    if (name.equalsIgnoreCase("id")) {
                        type = "INT";
                        isPrimary = true;
                    } else {
                        type = "TEXT";
                        isPrimary = false;
                    }
                    columns.add(new ColumnDefinition(name, type, isPrimary));
                } else if (parts.length >= 2) {
                    // 按照“name type [其它]”处理
                    String name = parts[0];
                    String type = parts[1];
                    if (parts.length > 2) {
                        StringBuilder typeBuilder = new StringBuilder(parts[1]);
                        for (int i = 2; i < parts.length; i++) {
                            typeBuilder.append(" ").append(parts[i]);
                        }
                        type = typeBuilder.toString();
                    }
                    columns.add(new ColumnDefinition(name, type, pk));
                }
            }
            return columns;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 读取表中所有记录（不含 schema 行）
     */
    public static List<List<String>> readTableRecords(String dbName, String tableName) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        List<List<String>> records = new ArrayList<>();
        if (!tableFile.exists()) {
            return records;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            // 跳过 schema 行
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                // 每行记录以制表符分隔
                String[] parts = line.split("\t");
                List<String> record = new ArrayList<>();
                for (String part : parts) {
                    record.add(part.trim());
                }
                records.add(record);
            }
        } catch (IOException e) {
            // 忽略读取错误
        }
        return records;
    }

    /**
     * 追加一条记录到表文件
     */
    public static boolean insertRow(String dbName, String tableName, String rowData) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) {
            return false;
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true))) {
            writer.write(rowData);
            writer.newLine();
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * 用新记录覆盖表文件（保留原 schema 行）
     */
    public static boolean writeTableRecords(String dbName, String tableName, List<ColumnDefinition> schema, List<List<String>> records) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) {
            return false;
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
            // 重构 schema 行，使用制表符分隔
            StringBuilder schemaLine = new StringBuilder();
            for (int i = 0; i < schema.size(); i++) {
                ColumnDefinition col = schema.get(i);
                // 这里只写入列名，如果需要写入类型信息可以自行调整
                schemaLine.append(col.getName());
                if (i < schema.size() - 1) {
                    schemaLine.append("\t");
                }
            }
            writer.write(schemaLine.toString());
            writer.newLine();
            // 写入每条记录，使用制表符拼接
            for (List<String> record : records) {
                writer.write(String.join("\t", record));
                writer.newLine();
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}
