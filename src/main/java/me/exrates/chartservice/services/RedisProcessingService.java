package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;

public interface RedisProcessingService {

    List<String> getAllKeys(BackDealInterval interval);

    boolean exists(String key, BackDealInterval interval);

    CandleModel get(String key, String hashKey, BackDealInterval interval);

    List<CandleModel> getAllByKey(String key, BackDealInterval interval);

    List<CandleModel> getByRange(LocalDateTime from, LocalDateTime to, String key, BackDealInterval interval);

    void batchInsertOrUpdate(List<CandleModel> models, String key, BackDealInterval interval);

    void insertOrUpdate(CandleModel model, String key, BackDealInterval interval);

    void deleteAll();

    void deleteByDbIndex(int dbIndex);

    void deleteByHashKey(String key, String hashKey, BackDealInterval interval);
}