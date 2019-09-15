package me.exrates.chartservice.services;

import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface ElasticsearchProcessingService {

    List<String> getAllIndices();

    boolean exists(String index, String id);

    List<CandleModel> get(String index, String id);

    Map<String, List<CandleModel>> getAllByIndex(String index);

    LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, String id);

    void bulkInsertOrUpdate(Map<String, List<CandleModel>> mapOfModels, String index);

    void insert(List<CandleModel> models, String index, String id);

    void update(List<CandleModel> models, String index, String id);

    long deleteAllData();

    long deleteDataByIndex(String index);

    void deleteAllIndices();

    void deleteIndex(String index);

    String createIndex(String index);

    boolean existsIndex(String index);
}