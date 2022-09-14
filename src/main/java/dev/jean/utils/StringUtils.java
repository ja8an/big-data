package dev.jean.utils;

import java.util.Locale;
import java.util.Objects;

public final class StringUtils {

    private StringUtils() {
    }

    /**
     * @param input string
     * @return slugified string
     */
    public static String slugify(final String input) {
        return Objects.requireNonNullElse(input, "") // Guarantees there will be an input
                .replaceAll("(\\W+)", "-") // Replaces all non word/number characters with '-'
                .replaceAll("([A-Z])", "-$1") // Adds '-' before capital letters
                .replaceAll("(\\w)(\\d+)", "$1-$2") // Adds a '-' before numbers succeeding letters
                .replaceAll("(-{2,})", "-") // Remove consecutive '-'
                .trim().toLowerCase(Locale.ROOT);
    }

}
