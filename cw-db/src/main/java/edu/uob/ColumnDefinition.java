package edu.uob;

import java.util.Objects;

/** 字段定义 Class to represent a column definition (name, type, primary key flag). */
public class ColumnDefinition {
    private String name;
    private String type;
    private boolean primaryKey;
    public ColumnDefinition(String name, String type, boolean primaryKey) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
    }
    public String getName() {
        return name;
    }
    public String getType() {
        return type;
    }
    public boolean isPrimaryKey() {
        return primaryKey;
    }
    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnDefinition)) return false;
        ColumnDefinition that = (ColumnDefinition) o;
        return name.equals(that.name);
    }
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}

