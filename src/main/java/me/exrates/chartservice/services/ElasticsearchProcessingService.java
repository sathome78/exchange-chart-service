package me.exrates.chartservice.services;

import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;

public interface ElasticsearchProcessingService {

    List<String> getAllIndices();

    boolean exists(String index, String id);

    CandleModel get(String index, String id);

    List<CandleModel> getAllByIndex(String index);

    List<CandleModel> getByRange(LocalDateTime fromDate, LocalDateTime toDate, String index);

    void batchInsertOrUpdate(List<CandleModel> models, String index);

    void insert(CandleModel model, String index);

    void update(CandleModel model, String index);

    long deleteAllData();

    long deleteDataByIndex(String index);

    void deleteAllIndices();

    void deleteIndex(String index);

    String createIndex(String index);

    boolean existsIndex(String index);
}