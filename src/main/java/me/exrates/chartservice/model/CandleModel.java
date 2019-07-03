package me.exrates.chartservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CandleModel {

    private BigDecimal openRate;
    private BigDecimal closeRate;
    private BigDecimal highRate;
    private BigDecimal lowRate;
    private BigDecimal volume;
    private LocalDateTime candleOpenTime;
}