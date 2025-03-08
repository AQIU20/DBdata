package edu.uob;

import java.util.ArrayList;
import java.util.List;

public class Tokenizer {
    /**
     * Tokenize a preprocessed SQL statement into a list of tokens.
     */
    public List<String> tokenize(String sql) {
        List<String> tokens = new ArrayList<>();
        if (sql == null) {
            return tokens;
        }
        // Split by whitespace to get initial tokens
        String[] parts = sql.trim().split(" ");
        StringBuilder stringToken = null;
        for (int i = 0; i < parts.length; i++) {
            String token = parts[i];
            if (token.isEmpty()) {
                continue;
            }
            // 如果 token 以单引号开始，处理字符串字面量
            if (token.startsWith("'")) {
                stringToken = new StringBuilder();
                token = token.substring(1); // 去掉前导引号
                if (token.endsWith("'")) {
                    token = token.substring(0, token.length() - 1); // 去掉尾部引号
                    stringToken.append(token);
                    // 保留原始大小写，并加上单引号以便后续处理
                    tokens.add("'" + stringToken.toString() + "'");
                    stringToken = null;
                } else {
                    stringToken.append(token);
                    // 继续合并后续 token，直到遇到结束引号
                    while (i + 1 < parts.length) {
                        i++;
                        String nextToken = parts[i];
                        if (nextToken.endsWith("'")) {
                            stringToken.append(" ").append(nextToken.substring(0, nextToken.length() - 1));
                            tokens.add("'" + stringToken.toString() + "'");
                            stringToken = null;
                            break;
                        } else {
                            stringToken.append(" ").append(nextToken);
                        }
                    }
                    if (stringToken != null) { // 如果结束引号缺失，也将其加入
                        tokens.add("'" + stringToken.toString() + "'");
                        stringToken = null;
                    }
                }
            } else {
                tokens.add(token);
            }
        }

        // 合并连续的比较操作符
        List<String> combinedTokens = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            String curr = tokens.get(i);
            if (i + 1 < tokens.size()) {
                String next = tokens.get(i + 1);
                if (curr.equals("=") && next.equals("=")) {
                    combinedTokens.add("==");
                    i++;
                    continue;
                }
                if (curr.equals(">") && next.equals("=")) {
                    combinedTokens.add(">=");
                    i++;
                    continue;
                }
                if (curr.equals("<") && next.equals("=")) {
                    combinedTokens.add("<=");
                    i++;
                    continue;
                }
                if (curr.equals("!") && next.equals("=")) {
                    combinedTokens.add("!=");
                    i++;
                    continue;
                }
            }
            combinedTokens.add(curr);
        }

        // 去除每个 token 尾部的分号
        List<String> finalTokens = new ArrayList<>();
        for (String t : combinedTokens) {
            if (t.endsWith(";")) {
                t = t.substring(0, t.length() - 1);
            }
            finalTokens.add(t);
        }

        return finalTokens;
    }
}
