package edu.uob;

public class CompoundCondition implements Condition {
    private Condition left;
    private String operator;
    private Condition right;

    public CompoundCondition(Condition left, String operator, Condition right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public Condition getLeft() {
        return left;
    }

    public String getOperator() {
        return operator;
    }

    public Condition getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "(" + left.toString() + " " + operator + " " + right.toString() + ")";
    }
}
