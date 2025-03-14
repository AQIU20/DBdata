package edu.uob;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Preprocessor {
    private static final Pattern STRING_LITERAL = Pattern.compile("'([^']*)'");

    public String preprocess(String sql) throws Exception {
        if (sql == null) return null;

        sql = sql.trim();
        if (!sql.endsWith(";")) {
            throw new Exception("Syntax error: missing semicolon.");
        }
        // 移除末尾的分号
        sql = sql.substring(0, sql.length() - 1);

        // 提取字符串字面量
        List<String> literals = extractStringLiterals(sql);
        String masked = maskLiterals(sql, literals);

        // 替换换行和制表符为单个空格，并折叠多余空格
        masked = masked.replace('\n', ' ').replace('\t', ' ');
        masked = masked.replaceAll("\\s+", " ");

        // (),
        masked = masked.replace("(", " ( ")
                .replace(")", " ) ")
                .replace(",", " , ");

        // 先处理组合比较操作符
        masked = masked.replaceAll(">=", " >= ")
                .replaceAll("<=", " <= ")
                .replaceAll("!=", " != ");
        // 再处理单字符比较操作符（若已存在空格，则不会重复添加）
        masked = masked.replace(">", " > ")
                .replace("<", " < ")
                .replace("=", " = ")
                .replace("*", " * ");


        String restored = restoreLiterals(masked, literals);
        return restored.replaceAll("\\s+", " ").trim();
    }

    private List<String> extractStringLiterals(String sql) {
        List<String> matches = new ArrayList<>();
        Matcher m = STRING_LITERAL.matcher(sql);
        while (m.find()) {
            matches.add(m.group());
        }
        return matches;
    }

    private String maskLiterals(String sql, List<String> literals) {
        String result = sql;
        for (int i = 0; i < literals.size(); i++) {
            result = result.replace(literals.get(i), "__LIT__" + i + "__");
        }
        return result;
    }

    private String restoreLiterals(String sql, List<String> literals) {
        String result = sql;
        for (int i = 0; i < literals.size(); i++) {
            result = result.replace("__LIT__" + i + "__", literals.get(i));
        }
        return result;
    }
}
