package edu.uob;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Preprocessor {
    /**
     * Preprocess an SQL statement by trimming whitespace, converting to upper case for keywords and identifiers,
     * while preserving the original case of string literals.
     * This helps to standardize the input for parsing.
     */
    public String preprocess(String sql) {
        if (sql == null) {
            return null;
        }

        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        // Replace newline and tab with space
        sql = sql.replace('\n', ' ').replace('\t', ' ');
        sql = sql.replaceAll("\\s+", " ");
        sql = sql.replace("(", " ( ");
        sql = sql.replace(")", " ) ");
        sql = sql.replace(",", " , ");
        sql = sql.replace("=", " = ");
        sql = sql.replace("*", " * ");
        // Collapse multiple spaces again after adding spaces around punctuation
        sql = sql.replaceAll("\\s+", " ");

        // 使用正则表达式提取所有单引号字符串，并用占位符替换
        Pattern pattern = Pattern.compile("'([^']*)'");
        Matcher matcher = pattern.matcher(sql);
        List<String> literals = new ArrayList<>();
        while (matcher.find()) {
            literals.add(matcher.group(0)); // 包括引号的字符串字面量
        }
        // 替换字面量为占位符
        for (int i = 0; i < literals.size(); i++) {
            sql = sql.replace(literals.get(i), "__STR" + i + "__");
        }

        // 转换除字面量以外的部分为大写
        sql = sql.toUpperCase();

        // 恢复原始的字符串字面量（保持原来的大小写）
        for (int i = 0; i < literals.size(); i++) {
            sql = sql.replace("__STR" + i + "__", literals.get(i));
        }

        return sql.trim();
    }
}
