package me.exrates.chartservice.repositories;

import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.CurrencyRateDto;
import me.exrates.chartservice.model.OrderDto;

import java.time.LocalDate;
import java.util.List;

public interface OrderRepository {

    List<CurrencyPairDto> getAllCurrencyPairs();

    List<CurrencyPairDto> getCurrencyPairByName(String pairName);

    List<CurrencyRateDto> getAllCurrencyRates();

    List<CurrencyRateDto> getCurrencyRateByName(String currencyName);

    List<OrderDto> getClosedOrders(LocalDate fromDate, LocalDate toDate, String pairName);
}