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
            // If a token begins with a single quote, handle potential string literal
            if (token.startsWith("'")) {
                stringToken = new StringBuilder();
                // Remove leading quote
                token = token.substring(1);
                // If token ends with a quote too (and isn't just that quote), then it's a one-word string literal
                if (token.endsWith("'")) {
                    // Remove trailing quote
                    token = token.substring(0, token.length() - 1);
                    stringToken.append(token);
                    tokens.add(stringToken.toString());
                    stringToken = null;
                } else {
                    // Start combining tokens until we find the closing quote
                    stringToken.append(token);
                    // Append a space if we will add more parts of the string literal
                    while (i + 1 < parts.length) {
                        i++;
                        String nextToken = parts[i];
                        // If this next token contains the closing quote at end
                        if (nextToken.endsWith("'")) {
                            // Include the intermediate space and the token without the closing quote
                            stringToken.append(" ").append(nextToken.substring(0, nextToken.length() - 1));
                            tokens.add(stringToken.toString());
                            stringToken = null;
                            break;
                        } else {
                            // Append the whole token with space and continue
                            stringToken.append(" ").append(nextToken);
                        }
                    }
                    // If stringToken is not null here, it means closing quote was missing.
                    // We will still add whatever we gathered as one token (it will be an incomplete string literal).
                    if (stringToken != null) {
                        tokens.add(stringToken.toString());
                        stringToken = null;
                    }
                }
            } else {
                // Not a string literal start, so just add token if not in middle of string assembly
                tokens.add(token);
            }
        }

        // 合并连续的 "=" token：如果相邻两个 token 均为 "=" 则合并为 "=="
        List<String> combinedTokens = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            String curr = tokens.get(i);
            if (curr.equals("=") && (i + 1 < tokens.size()) && tokens.get(i + 1).equals("=")) {
                combinedTokens.add("==");
                i++; // 跳过下一个 token
            } else {
                combinedTokens.add(curr);
            }
        }
        return combinedTokens;
    }
}
