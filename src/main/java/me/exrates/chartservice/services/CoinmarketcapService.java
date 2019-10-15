package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CoinmarketcapApiDto;

import java.util.List;

public interface CoinmarketcapService {

    void cleanDailyData();

    void generate();

    List<CoinmarketcapApiDto> getData(String pairName, BackDealInterval interval);
}