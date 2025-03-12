package edu.uob;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StorageManager {
    private static final String BASE_PATH = "databases";


    public static boolean databaseExists(String dbName) {
        File dbDir = new File(BASE_PATH, dbName);
        return dbDir.exists() && dbDir.isDirectory();
    }


    public static boolean createDatabase(String dbName) {
        File dbDir = new File(BASE_PATH, dbName);
        if (dbDir.exists()) return false;
        return dbDir.mkdir();
    }


    public static boolean deleteDatabase(String dbName) {
        File dbDir = new File(BASE_PATH, dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) return false;
        File[] files = dbDir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (!f.delete()) return false;
            }
        }
        return dbDir.delete();
    }


    public static boolean tableExists(String dbName, String tableName) {
        if (dbName == null) return false;
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        return tableFile.exists() && tableFile.isFile();
    }


    public static boolean createTable(String dbName, String tableName, String schemaLine) {
        if (dbName == null) return false;
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (tableFile.exists()) return false;


        String processed = schemaLine.replaceAll("\\s*,\\s*", "\t");
        String newSchema = "id\t" + processed;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
            writer.write(newSchema);
            writer.newLine();
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    public static boolean deleteTable(String dbName, String tableName) {
        if (dbName == null) return false;
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        return tableFile.exists() && tableFile.delete();
    }


    public static List<ColumnDefinition> readTableSchema(String dbName, String tableName) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) return null;

        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String schemaLine = reader.readLine();
            if (schemaLine == null) return null;

            List<ColumnDefinition> columns = new ArrayList<>();
            String[] colDefs = schemaLine.split("\t");

            for (String def : colDefs) {
                def = def.trim();
                if (def.isEmpty()) continue;


                boolean pk = def.toUpperCase().contains("PRIMARY KEY");
                String cleanDef = def.replaceAll("(?i)PRIMARY KEY", "").trim();
                String[] parts = cleanDef.split("\\s+");


                if (parts.length == 1) {
                    String name = parts[0];
                    if (name.equalsIgnoreCase("id")) {
                        columns.add(new ColumnDefinition(name, "INT", true));
                    } else {
                        columns.add(new ColumnDefinition(name, "TEXT", false));
                    }
                } else if (parts.length >= 2) {
                    String type = parts[1];
                    if (parts.length > 2) {
                        StringBuilder typeBuilder = new StringBuilder(parts[1]);
                        for (int i = 2; i < parts.length; i++) {
                            typeBuilder.append(" ").append(parts[i]);
                        }
                        type = typeBuilder.toString();
                    }
                    columns.add(new ColumnDefinition(parts[0], type, pk));
                }
            }
            return columns;
        } catch (IOException e) {
            return null;
        }
    }

    public static List<List<String>> readTableRecords(String dbName, String tableName) {
        List<List<String>> records = new ArrayList<>();
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) return records;

        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            reader.readLine(); // Skip schema
            String line;
            while ((line = reader.readLine()) != null) {
                List<String> record = new ArrayList<>();
                for (String part : line.split("\t")) {
                    record.add(part.trim());
                }
                records.add(record);
            }
        } catch (IOException e) {

        }
        return records;
    }


    public static boolean insertRow(String dbName, String tableName, String rowData) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) return false;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true))) {
            writer.write(rowData);
            writer.newLine();
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    public static boolean writeTableRecords(String dbName, String tableName,
                                            List<ColumnDefinition> schema, List<List<String>> records) {
        File tableFile = new File(new File(BASE_PATH, dbName), tableName + ".txt");
        if (!tableFile.exists()) return false;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {

            StringBuilder schemaLine = new StringBuilder();
            for (int i = 0; i < schema.size(); i++) {
                schemaLine.append(schema.get(i).getName());
                if (i < schema.size() - 1) schemaLine.append("\t");
            }
            writer.write(schemaLine.toString());
            writer.newLine();


            for (List<String> record : records) {
                writer.write(String.join("\t", record));
                writer.newLine();
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}