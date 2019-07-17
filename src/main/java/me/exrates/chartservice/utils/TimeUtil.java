package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class TimeUtil {

    private static final BackDealInterval DEFAULT_MIN_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    public static LocalDateTime getNearestTimeBeforeForMinInterval(LocalDateTime dateTime) {
        return getNearestBackTimeForBackdealInterval(dateTime, DEFAULT_MIN_INTERVAL);
    }

    public static LocalDateTime getNearestBackTimeForBackdealInterval(LocalDateTime dateTime, BackDealInterval interval) {
        switch (interval.getIntervalType()) {
            case MINUTE: {
                return dateTime.truncatedTo(ChronoUnit.HOURS)
                        .plusMinutes(interval.getIntervalValue() * (dateTime.getMinute() / interval.getIntervalValue()));
            }
            case HOUR: {
                return dateTime.truncatedTo(ChronoUnit.DAYS)
                        .plusHours(interval.getIntervalValue() * (dateTime.getHour() / interval.getIntervalValue()));
            }
            case DAY: {
                return dateTime.truncatedTo(ChronoUnit.DAYS).minusDays(dateTime.getDayOfYear())
                        .plusDays(interval.getIntervalValue() * (dateTime.getDayOfYear() / interval.getIntervalValue()));
            }
            default: {
                throw new UnsupportedOperationException(String.format("Interval type - %s not supported", interval.getIntervalType()));
            }
        }
    }

    public static int convertToMinutes(BackDealInterval interval) {
        switch (interval.getIntervalType()) {
            case MINUTE: {
                return interval.getIntervalValue();
            }
            case HOUR: {
                return interval.getIntervalValue() * 60;
            }
            case DAY: {
                return interval.getIntervalValue() * 24 * 60;
            }
            default: {
                throw new UnsupportedOperationException(String.format("Interval type - %s not supported", interval.getIntervalType()));
            }
        }
    }
}
