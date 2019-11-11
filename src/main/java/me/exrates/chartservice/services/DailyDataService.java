package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.ExchangeRatesDto;

import java.util.List;

public interface DailyDataService {

    List<CoinmarketcapApiDto> getCoinmarketcapData(String pairName, BackDealInterval interval);

    List<ExchangeRatesDto> getExchangeRatesData(String pairName, BackDealInterval interval);
}