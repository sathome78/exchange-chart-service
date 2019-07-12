package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;
import me.exrates.chartservice.model.TradeDataDto;

import java.time.LocalDateTime;
import java.util.List;

public interface TradeDataService {

    CandleModel getCandleForCurrentTime(String pairName, BackDealInterval interval);

    CandlesDataDto getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval);


    void handleReceivedTrades(String pairname, List<TradeDataDto> dto);

    LocalDateTime getLastInitializedCandleTime();
}
