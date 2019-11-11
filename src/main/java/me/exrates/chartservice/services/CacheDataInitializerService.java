package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.util.List;

public interface CacheDataInitializerService {

    void updateCacheByIndexAndId(String index, String id);

    void updateCache(List<CandleModel> models, String key, String hashKey, BackDealInterval interval);

    void cleanCache();

    void cleanCache(BackDealInterval interval);

    void cleanDailyData();
}