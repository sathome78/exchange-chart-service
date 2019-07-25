package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.util.List;

public interface CacheDataInitializerService {

    void updateCache();

    void updateCacheByKey(String key);

    void updateCache(List<CandleModel> models, String key, BackDealInterval interval);

    void cleanCache();

    void cleanCache(BackDealInterval interval);
}