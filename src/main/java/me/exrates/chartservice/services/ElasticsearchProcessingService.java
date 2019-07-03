package me.exrates.chartservice.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public interface ElasticsearchProcessingService {

    void send(CandleModel model, String pairName);

    List<CandleModel> get(LocalDateTime fromDate, LocalDateTime toDate, String pairName);
}