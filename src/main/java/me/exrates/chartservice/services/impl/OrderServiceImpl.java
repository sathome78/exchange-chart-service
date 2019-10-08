package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.CurrencyRateDto;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.repositories.OrderRepository;
import me.exrates.chartservice.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

import static me.exrates.chartservice.configuration.CacheConfiguration.CURRENCY_PAIRS_CACHE;
import static me.exrates.chartservice.configuration.CacheConfiguration.CURRENCY_RATES_CACHE;

@Log4j2
@Transactional
@Service
public class OrderServiceImpl implements OrderService {

    private static final String ALL = "ALL";

    private final OrderRepository orderRepository;
    private final Cache currencyPairsCache;
    private final Cache currencyRatesCache;

    @Autowired
    public OrderServiceImpl(OrderRepository orderRepository,
                            @Qualifier(CURRENCY_PAIRS_CACHE) Cache currencyPairsCache,
                            @Qualifier(CURRENCY_RATES_CACHE) Cache currencyRatesCache) {
        this.orderRepository = orderRepository;
        this.currencyPairsCache = currencyPairsCache;
        this.currencyRatesCache = currencyRatesCache;
    }

    @Override
    public List<CurrencyPairDto> getCurrencyPairsFromCache(String pairName) {
        return Objects.nonNull(pairName)
                ? currencyPairsCache.get(pairName, () -> getCurrencyPairByName(pairName))
                : currencyPairsCache.get(ALL, this::getAllCurrencyPairs);
    }

    @Transactional(readOnly = true)
    @Override
    public List<CurrencyPairDto> getAllCurrencyPairs() {
        return orderRepository.getAllCurrencyPairs();
    }

    @Transactional(readOnly = true)
    @Override
    public List<CurrencyPairDto> getCurrencyPairByName(String pairName) {
        return orderRepository.getCurrencyPairByName(pairName);
    }

    @Override
    public List<CurrencyRateDto> getAllCurrencyRatesFromCache(String currencyName) {
        return Objects.nonNull(currencyName)
                ? currencyRatesCache.get(currencyName, () -> getCurrencyRateByName(currencyName))
                : currencyRatesCache.get(ALL, this::getAllCurrencyRates);
    }

    @Transactional(readOnly = true)
    @Override
    public List<CurrencyRateDto> getAllCurrencyRates() {
        return orderRepository.getAllCurrencyRates();
    }

    @Override
    public List<CurrencyRateDto> getCurrencyRateByName(String currencyName) {
        return orderRepository.getCurrencyRateByName(currencyName);
    }

    @Transactional(readOnly = true)
    @Override
    public List<OrderDto> getClosedOrders(LocalDate fromDate, LocalDate toDate, String pairName) {
        return orderRepository.getClosedOrders(fromDate, toDate, pairName);
    }
}