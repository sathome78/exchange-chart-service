package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;

public interface RedisProcessingService {

    CandleModel get(String pairName, LocalDateTime dateTime, BackDealInterval interval);

    List<CandleModel> getByRange(LocalDateTime from, LocalDateTime to, String pairName, BackDealInterval interval);

    void batchInsertOrUpdate(List<CandleModel> models, String pairName, BackDealInterval interval);

    void insertOrUpdate(CandleModel model, String pairName, BackDealInterval interval);

    void deleteAll();

    void deleteByIndex(int index);
}