package edu.uob;

import edu.uob.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.List;

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
     * KEEP this signature otherwise we won't be able to mark your submission correctly.
     */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist!
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
    }

    /**
     * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
     * able to mark your submission correctly.
     *
     * <p>This method handles all incoming DB commands and carries out the required actions.
     */
    public String handleCommand(String command) {
        try {
            // 1. 预处理命令
            String formatted = new Preprocessor().preprocess(command);

            // 2. 词法分析（Tokenize）
            List<String> tokens = new Tokenizer().tokenize(formatted);
            if (tokens.isEmpty()) {
                return "";
            }

            // 3. 语法解析（Parse）
            SQLStatement statement = new Parser().parse(tokens);

            // 4. 语义分析（Semantic Analysis）
            new SemanticAnalyzer().validate(statement);

            // 5. 执行对应的数据库命令
            String result = "";
            if (statement instanceof CreateDatabaseStatement) {
                result = DatabaseManager.createDatabase(((CreateDatabaseStatement) statement).getDatabaseName());
            } else if (statement instanceof DropDatabaseStatement) {
                result = DatabaseManager.dropDatabase(((DropDatabaseStatement) statement).getDatabaseName());
            } else if (statement instanceof UseDatabaseStatement) {
                result = DatabaseManager.useDatabase(((UseDatabaseStatement) statement).getDatabaseName());
            } else if (statement instanceof CreateTableStatement) {
                CreateTableStatement stmt = (CreateTableStatement) statement;
                result = TableManager.createTable(stmt.getTableName(), stmt.getColumns());
            } else if (statement instanceof DropTableStatement) {
                result = TableManager.dropTable(((DropTableStatement) statement).getTableName());
            } else if (statement instanceof InsertStatement) {
                result = QueryExecutor.executeInsert((InsertStatement) statement);
            } else if (statement instanceof SelectStatement) {
                result = QueryExecutor.executeSelect((SelectStatement) statement);
            } else if (statement instanceof UpdateStatement) {
                result = QueryExecutor.executeUpdate((UpdateStatement) statement);
            } else if (statement instanceof DeleteStatement) {
                result = QueryExecutor.executeDelete((DeleteStatement) statement);
            } else if (statement instanceof JoinStatement) {
                result = QueryExecutor.executeJoin((JoinStatement) statement);
            } else if (statement instanceof AlterTableStatement) {
                result = QueryExecutor.executeAlter((AlterTableStatement) statement);
            } else {
                result = ErrorHandler.syntaxError();
            }

            // 如果没有错误，则在返回结果前加上 [OK] 标签
            if (!result.contains("[ERROR]")) {
                result = "[OK] " + result;
            }
            return result;
        } catch (Exception e) {
            // 修改处：返回错误信息时添加 [ERROR] 标签
            return "[ERROR] " + e.getMessage();
        }
    }

    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }
}
