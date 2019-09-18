package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface RedisProcessingService {

    List<String> getAllKeys(BackDealInterval interval);

    boolean exists(String key, BackDealInterval interval);

    boolean exists(String key, int dbIndex);

    boolean exists(String key, String hashKey, BackDealInterval interval);

    List<CandleModel> get(String key, String hashKey, BackDealInterval interval);

    LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, LocalDateTime boundaryTime, String hashKey, BackDealInterval interval);

    void bulkInsertOrUpdate(Map<String, List<CandleModel>> mapOfModels, String hashKey, BackDealInterval interval);

    void insertOrUpdate(List<CandleModel> models, String key, String hashKey, BackDealInterval interval);

    void deleteAllKeys();

    void deleteKey(String key);

    void deleteKeyByDbIndexAndKey(int dbIndex, String key);

    void deleteDataByHashKey(String key, String hashKey, BackDealInterval interval);

    void insertLastInitializedCandleTimeToCache(String key, LocalDateTime dateTime);

    LocalDateTime getLastInitializedCandleTimeFromCache(String key);

    void insertFirstInitializedCandleTimeToHistory(String key, LocalDateTime dateTime);

    LocalDateTime getFirstInitializedCandleTimeFromHistory(String key);
}