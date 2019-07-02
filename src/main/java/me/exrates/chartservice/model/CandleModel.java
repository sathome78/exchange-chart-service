package me.exrates.chartservice.model;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class CandleModel {

    private BigDecimal openRate;
    private BigDecimal closeRate;
    private BigDecimal highRate;
    private BigDecimal lowRate;
    private BigDecimal volume;
    private LocalDateTime candleOpenTime;

}
