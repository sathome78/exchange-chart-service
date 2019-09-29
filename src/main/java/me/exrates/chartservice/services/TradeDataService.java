package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDataDto;

import java.time.LocalDateTime;
import java.util.List;

public interface TradeDataService {

    CandleModel getCandleForCurrentTime(String pairName, BackDealInterval interval);

    List<CandleModel> getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval);

    LocalDateTime getLastCandleTimeBeforeDate(String pairName, LocalDateTime candleDateTime, BackDealInterval interval);

    void handleReceivedTrades(String pairname, List<OrderDataDto> dto);

    void defineAndSaveLastInitializedCandleTime(String hashKey, List<CandleModel> models);

    void defineAndSaveFirstInitializedCandleTime(String hashKey, List<CandleModel> models);
}
