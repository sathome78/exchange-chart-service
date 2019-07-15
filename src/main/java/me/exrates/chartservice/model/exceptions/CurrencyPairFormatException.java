package me.exrates.chartservice.model.exceptions;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class CurrencyPairFormatException extends RuntimeException {

    public CurrencyPairFormatException(String message) {
        super(message);
    }
}