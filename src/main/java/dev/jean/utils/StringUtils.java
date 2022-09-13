package dev.jean.utils;

import java.util.Locale;

public class StringUtils {

    public static String slugify(String input) {
        return input
                .replaceAll("(\\W+)", "-")
                .replaceAll("([A-Z])", "-$1")
                .replaceAll("(\\w)(\\d+)", "$1-$2")
                .replaceAll("(-{2,})", "-")
                .replaceAll("(-$|^-)", "")
                .toLowerCase(Locale.ROOT);
    }

}
