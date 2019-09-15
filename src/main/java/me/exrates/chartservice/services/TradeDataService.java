package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.ModelList;
import me.exrates.chartservice.model.TradeDataDto;

import java.time.LocalDateTime;
import java.util.List;

public interface TradeDataService {

    CandleModel getCandleForCurrentTime(String pairName, BackDealInterval interval);

    List<CandleModel> getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval);

    LocalDateTime getLastCandleTimeBeforeDate(String pairName, LocalDateTime candleDateTime, BackDealInterval interval);

    void handleReceivedTrades(String pairname, List<TradeDataDto> dto);

    void defineAndSaveLastInitializedCandleTime(String hashKey, List<CandleModel> models);
}
