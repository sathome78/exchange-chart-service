package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;

/*contract - interface, specification of needed methods*/
public interface CacheInterface {

    CandleModel getCandle(String pairName, LocalDateTime localDateTime, BackDealInterval backDealInterval);

    List<CandleModel> getCandlesRange(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval);

    CandleModel updateCandle(String pairName, LocalDateTime localDateTime, BackDealInterval backDealInterval);

    CandleModel insertCandle(String pairName, LocalDateTime localDateTime, BackDealInterval backDealInterval);
}
