package me.exrates.chartservice.services;

import me.exrates.chartservice.model.CandleModel;

import java.time.LocalDateTime;
import java.util.List;

public interface ElasticsearchProcessingService {

    boolean exist(String pairName, LocalDateTime dateTime);

    CandleModel get(String pairName, LocalDateTime dateTime);

    void insert(CandleModel model, String pairName);

    void update(CandleModel model, String pairName);

    long deleteAll();

    List<CandleModel> getByQuery(LocalDateTime fromDate, LocalDateTime toDate, String pairName);
}