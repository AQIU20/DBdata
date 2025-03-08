package edu.uob;

public class SimpleCondition implements Condition {
    private String attribute;
    private String comparator;
    private String value;

    public SimpleCondition(String attribute, String comparator, String value) {
        this.attribute = attribute;
        this.comparator = comparator;
        this.value = value;
    }

    public String getAttribute() {
        return attribute;
    }

    public String getComparator() {
        return comparator;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return attribute + " " + comparator + " " + value;
    }
}
