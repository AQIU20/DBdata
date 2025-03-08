package edu.uob;

public class JoinStatement implements SQLStatement {
    private String table1;
    private String table2;
    private String attribute1;
    private String attribute2;

    public JoinStatement(String table1, String table2, String attribute1, String attribute2) {
        this.table1 = table1;
        this.table2 = table2;
        this.attribute1 = attribute1;
        this.attribute2 = attribute2;
    }

    public String getTable1() {
        return table1;
    }

    public String getTable2() {
        return table2;
    }

    public String getAttribute1() {
        return attribute1;
    }

    public String getAttribute2() {
        return attribute2;
    }
}
