package me.exrates.chartservice.model.enums;

import lombok.Getter;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

@Getter
public enum IntervalType {

    MINUTE(ChronoUnit.MINUTES, 5, 15, 30),
    HOUR(ChronoUnit.HOURS, 1, 6),
    DAY(ChronoUnit.DAYS, 1);

    private TemporalUnit correspondingTimeUnit;

    private int[] supportedValues;

    IntervalType(TemporalUnit correspondingTimeUnit, int... supportedValues) {
        this.correspondingTimeUnit = correspondingTimeUnit;
        this.supportedValues = supportedValues;
    }
}