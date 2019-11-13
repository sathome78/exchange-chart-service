package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

    public static String convertToResolution(BackDealInterval interval) {
        Integer intervalValue = interval.getIntervalValue();

        switch (interval.getIntervalType()) {
            case MINUTE: {
                return String.valueOf(intervalValue);
            }
            case HOUR: {
                return String.valueOf(intervalValue * 60);
            }
            case DAY: {
                return intervalValue == 1 ? "D" : String.valueOf(intervalValue).concat("D");
            }
            default: {
                throw new UnsupportedOperationException(String.format("Interval type - %s not supported", interval.getIntervalType()));
            }
        }
    }

    public static BackDealInterval getInterval(String resolution) {
        IntervalType type;
        int value;

        if (resolution.contains("H")) {
            type = IntervalType.HOUR;
            value = getValue(resolution, "H");
        } else if (resolution.contains("D")) {
            type = IntervalType.DAY;
            value = getValue(resolution, "D");
        } else {
            type = IntervalType.MINUTE;
            value = Integer.valueOf(resolution);
        }
        return new BackDealInterval(value, type);
    }

    private static int getValue(String resolution, String type) {
        String strValue = resolution.replace(type, StringUtils.EMPTY);

        return strValue.equals(StringUtils.EMPTY)
                ? 1
                : Integer.valueOf(strValue);
    }

    public static LocalDateTime getTime(long time) {
        return LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC);
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

    public static LocalDate getBoundaryTime(long candlesToStoreInCache, BackDealInterval interval) {
        LocalDateTime currentDateTime = LocalDateTime.now();

        return currentDateTime.minusMinutes(candlesToStoreInCache * TimeUtil.convertToMinutes(interval)).toLocalDate();
    }
}