package dev.jean.tde1;

import dev.jean.base.BaseHadoop;
import lombok.SneakyThrows;

public class Main {
    public static void main(final String[] args) {
        System.exit(
                run(Activity1.class) ? 0 : 1
        );
    }

    @SneakyThrows
    public static boolean run(final Class<? extends BaseHadoop<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>> classFile) {
        BaseHadoop<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> baseHadoop = classFile.getConstructor().newInstance();
        return baseHadoop.run(true);
    }
}
