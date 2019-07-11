package me.exrates.chartservice.services;

import java.time.LocalDate;

public interface DataInitializerService {

    void generate(LocalDate fromDate, LocalDate toDate, String pairName, boolean regenerate);
}