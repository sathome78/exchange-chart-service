package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class TimeUtil {

    private static final BackDealInterval DEFAULT_MIN_INTERVAL = new BackDealInterval("5 MINUTE");

    public static LocalDateTime getNearestTimeBeforeForMinInterval(LocalDateTime dateTime) {
        return getNearestBackTimeForBackdealInterval(dateTime, DEFAULT_MIN_INTERVAL);
    }

    public static LocalDateTime getNearestBackTimeForBackdealInterval(LocalDateTime dateTime, BackDealInterval backDealInterval) {
        switch (backDealInterval.getIntervalType()) {
            case MINUTE: {
                return dateTime.truncatedTo(ChronoUnit.HOURS)
                        .plusMinutes(backDealInterval.getIntervalValue() * (dateTime.getMinute() / backDealInterval.getIntervalValue()));
            }
            case HOUR: {
                return dateTime.truncatedTo(ChronoUnit.DAYS)
                        .plusHours(backDealInterval.getIntervalValue() * (dateTime.getHour() / backDealInterval.getIntervalValue()));
            }
            case DAY: {
                return dateTime.truncatedTo(ChronoUnit.DAYS).minusDays(dateTime.getDayOfYear())
                        .plusDays(backDealInterval.getIntervalValue() * (dateTime.getDayOfYear() / backDealInterval.getIntervalValue()));
            }
            default: {
                throw new UnsupportedOperationException(String.format("Interval type - %s not suuported", backDealInterval.getIntervalType()));
            }
        }
    }
}