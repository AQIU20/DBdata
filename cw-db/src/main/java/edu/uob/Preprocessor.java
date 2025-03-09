package edu.uob;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Preprocessor {
    private static final Pattern STRING_LITERAL = Pattern.compile("'([^']*)'");

    public String preprocess(String sql) {
        if (sql == null) return null;

        sql = sql.trim().replaceAll(";$", "");
        List<String> literals = extractStringLiterals(sql);
        String masked = maskLiterals(sql, literals);

        // 保持原始替换顺序
        masked = masked.replaceAll("\\s+", " ")
                .replace("(", " ( ")
                .replace(")", " ) ")
                .replace(",", " , ")
                .replace("=", " = ")
                .replace("*", " * ");

        masked = masked.toUpperCase();
        return restoreLiterals(masked, literals).replaceAll("\\s+", " ").trim();
    }

    private List<String> extractStringLiterals(String sql) {
        List<String> matches = new ArrayList<>();
        Matcher m = STRING_LITERAL.matcher(sql);
        while (m.find()) matches.add(m.group());
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