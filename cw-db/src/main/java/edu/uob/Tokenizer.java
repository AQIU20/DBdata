package edu.uob;

import java.util.ArrayList;
import java.util.List;

public class Tokenizer {
    //Tokenize into a list of tokens.

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
            // begin with '
            if (token.startsWith("'")) {
                stringToken = new StringBuilder();
                token = token.substring(1);
                if (token.endsWith("'")) {
                    token = token.substring(0, token.length() - 1);
                    stringToken.append(token);
                    // raw word + '
                    tokens.add("'" + stringToken.toString() + "'");
                    stringToken = null;
                } else {
                    stringToken.append(token);
                    // merge token
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
                    if (stringToken != null) {
                        tokens.add("'" + stringToken.toString() + "'");
                        stringToken = null;
                    }
                }
            } else {
                tokens.add(token);
            }
        }

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
