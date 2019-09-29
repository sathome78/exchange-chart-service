package me.exrates.chartservice.repositories;

import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.OrderDto;

import java.time.LocalDate;
import java.util.List;

public interface OrderRepository {

    List<CurrencyPairDto> getAllCurrencyPairNames();

    List<OrderDto> getFilteredOrders(LocalDate fromDate, LocalDate toDate, String pairName);
}