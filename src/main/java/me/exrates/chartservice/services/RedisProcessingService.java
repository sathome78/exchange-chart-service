package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;

public interface RedisProcessingService {

    List<String> getAllKeys(BackDealInterval interval);

    boolean exists(String key, BackDealInterval interval);

    boolean exists(String key, int dbIndex);

    CandleModel get(String key, String hashKey, BackDealInterval interval);

    List<CandleModel> getAllByKey(String key, BackDealInterval interval);

    List<CandleModel> getByRange(LocalDateTime from, LocalDateTime to, String key, BackDealInterval interval);

    LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime date, String key, BackDealInterval interval);

    void bulkInsertOrUpdate(List<CandleModel> models, String key, BackDealInterval interval);

    void insertOrUpdate(CandleModel model, String key, BackDealInterval interval);

    void deleteAllKeys();

    void deleteKey(String key);

    void deleteKeyByDbIndexAndKey(int dbIndex, String key);

    void deleteDataByHashKey(String key, String hashKey, BackDealInterval interval);

    void insertLastInitializedCandleTimeToCache(String key, LocalDateTime dateTime);

    LocalDateTime getLastInitializedCandleTimeFromCache(String key);
}