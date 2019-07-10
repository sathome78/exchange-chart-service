package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static me.exrates.chartservice.utils.TimeUtils.getNearestTimeBefore;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CandleModel {

    @JsonProperty("open_rate")
    private BigDecimal openRate;
    @JsonProperty("closed_rate")
    private BigDecimal closeRate;
    @JsonProperty("high_rate")
    private BigDecimal highRate;
    @JsonProperty("low_rate")
    private BigDecimal lowRate;
    private BigDecimal volume;
    @JsonProperty("last_trade_time")
    private LocalDateTime lastTradeTime;
    @JsonProperty("candle_open_time")
    private LocalDateTime candleOpenTime;
    @JsonProperty("time_in_millis")
    private long timeInMillis;

    public long getTimeInMillis() {
        return Timestamp.valueOf(candleOpenTime).getTime();
    }
    private LocalDateTime lastTradeTime;

    public static CandleModel newCandleFromTrade(TradeDataDto dto) {
        return CandleModel.builder()
                .candleOpenTime(getNearestTimeBefore(30, dto.getTradeDate()))
                .openRate(dto.getExrate())
                .closeRate(dto.getExrate())
                .highRate(dto.getExrate())
                .lowRate(dto.getExrate())
                .volume(dto.getAmountBase())
                .lastTradeTime(dto.getTradeDate())
                .build();
    }
}

