package dev.jean.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum InputFile {
    TRANSACTIONS("in/transactions_amostra.csv");
    private final String path;
}
