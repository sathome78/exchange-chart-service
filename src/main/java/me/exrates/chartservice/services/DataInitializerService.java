package me.exrates.chartservice.services;

import java.time.LocalDate;
import java.util.List;

public interface DataInitializerService {

    void generateCandleData(LocalDate fromDate, LocalDate toDate);

    void generateCandleData(LocalDate fromDate, LocalDate toDate, List<String> pairs);

    void generateCandleData(LocalDate fromDate, LocalDate toDate, String pair);

    void generateDailyData();
}