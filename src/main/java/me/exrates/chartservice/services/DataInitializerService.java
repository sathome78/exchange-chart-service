package me.exrates.chartservice.services;

import java.time.LocalDate;
import java.util.List;

public interface DataInitializerService {

    void generate(LocalDate fromDate, LocalDate toDate);

    void generate(LocalDate fromDate, LocalDate toDate, List<String> pairs);

    void generate(LocalDate fromDate, LocalDate toDate, String pair);
}