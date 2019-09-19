package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.exrates.chartservice.model.BackDealInterval;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.DEFAULT_INTERVAL;

@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public final class TimeUtil {

    private static final DateTimeFormatter FORMATTER_DATE = DateTimeFormatter.ofPattern("dd_MM_yyyy");
    private static final DateTimeFormatter FORMATTER_DATE_TIME = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss");

    public static LocalDateTime getNearestTimeBeforeForMinInterval(LocalDateTime dateTime) {
        return getNearestBackTimeForBackdealInterval(dateTime, DEFAULT_INTERVAL);
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

    public static LocalDate generateDate(String dateString) {
        if (isNull(dateString)) {
            return null;
        }
        try {
            return LocalDate.parse(dateString, FORMATTER_DATE);
        } catch (Exception ex) {
            log.error("Process of parsing string to date format occurred error", ex);
            return null;
        }
    }

    public static LocalDateTime generateDateTime(String dateTimeString) {
        if (isNull(dateTimeString)) {
            return null;
        }
        try {
            return LocalDateTime.parse(dateTimeString, FORMATTER_DATE_TIME);
        } catch (Exception ex) {
            log.error("Process of parsing string to date format occurred error", ex);
            return null;
        }
    }

    public static String generateDateTimeString(LocalDateTime dateTime) {
        return dateTime.format(FORMATTER_DATE_TIME);
    }
}