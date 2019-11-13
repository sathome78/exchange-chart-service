package me.exrates.chartservice.services;

import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.CurrencyRateDto;
import me.exrates.chartservice.model.DailyDataDto;
import me.exrates.chartservice.model.OrderDto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public interface OrderService {

    List<CurrencyPairDto> getCurrencyPairsFromCache(String pairName);

    List<CurrencyPairDto> getAllCurrencyPairs();

    List<CurrencyPairDto> getCurrencyPairByName(String pairName);

    List<CurrencyRateDto> getAllCurrencyRatesFromCache(String pairName);

    List<CurrencyRateDto> getAllCurrencyRates();

    List<CurrencyRateDto> getCurrencyRateByName(String pairName);

    List<OrderDto> getClosedOrders(LocalDate from, LocalDate to, String pairName);

    List<DailyDataDto> getDailyData(String pairName);
}