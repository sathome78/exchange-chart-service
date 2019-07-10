package me.exrates.chartservice.model.enums;

import lombok.Getter;
import me.exrates.chartservice.model.exceptions.UnsupportedIntervalTypeException;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.stream.IntStream;

@Getter
public enum IntervalType {

    MINUTE(ChronoUnit.MINUTES, 5, 15, 30),
    HOUR(ChronoUnit.HOURS, 1, 6),
    DAY(ChronoUnit.DAYS, 1),
    WEEK(ChronoUnit.WEEKS),
    MONTH(ChronoUnit.MONTHS);

    private TemporalUnit correspondingTimeUnit;

    private int[] supportedValues;

    IntervalType(TemporalUnit correspondingTimeUnit, int... supportedValues) {
        this.correspondingTimeUnit = correspondingTimeUnit;
        this.supportedValues = supportedValues;
    }

    public static IntervalType convert(String str, int intervalValue) {
        return Arrays.stream(IntervalType.values())
                .filter(val -> (val.name().equals(str) && IntStream.of(val.supportedValues).anyMatch(x -> x == intervalValue)))
                .findFirst()
                .orElseThrow(() -> new UnsupportedIntervalTypeException(str));
    }
}
