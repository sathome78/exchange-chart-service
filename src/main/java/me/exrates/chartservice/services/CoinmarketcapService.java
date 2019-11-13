package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CoinmarketcapApiDto;

import java.util.List;

public interface CoinmarketcapService {

    List<CoinmarketcapApiDto> getData(String pairName, BackDealInterval interval);
}