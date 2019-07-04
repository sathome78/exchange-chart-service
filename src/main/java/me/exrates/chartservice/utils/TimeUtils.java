package me.exrates.chartservice.utils;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class TimeUtils {

    private static final int DEFAULT_INTERVAL_MINUTES = 30;

    private TimeUtils() {
    }

    public static LocalDateTime getNearestTimeBefore(LocalDateTime dateTime) {
        return getNearestTimeBefore(DEFAULT_INTERVAL_MINUTES, dateTime);
    }

    public static LocalDateTime getNearestTimeBefore(int minutesToRoundFor, LocalDateTime dateTime) {
        return dateTime.truncatedTo(ChronoUnit.HOURS)
                .plusMinutes(minutesToRoundFor * (dateTime.getMinute() / minutesToRoundFor));
    }

    /*todo remove and write tests*/
    public static void main(String[] args) {
        System.out.println(getNearestTimeBefore(30, LocalDateTime.now()));
    }

}
