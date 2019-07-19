package me.exrates.chartservice.services;

public interface CacheDataInitializerService {

    void updateCache();

    void updateCacheByKey(String key);

    void cleanCache();
}