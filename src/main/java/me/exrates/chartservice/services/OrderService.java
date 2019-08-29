package me.exrates.chartservice.services;

import me.exrates.chartservice.model.OrderDto;

import java.time.LocalDate;
import java.util.List;

public interface OrderService {

    List<String> getAllCurrencyPairNames();

    List<OrderDto> getFilteredOrders(LocalDate fromDate, LocalDate toDate, String pairName);
}