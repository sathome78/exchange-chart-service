package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonProperty("pair_name")
    private String pairName;
    @JsonProperty("open")
    private BigDecimal openRate;
    @JsonProperty("close")
    private BigDecimal closeRate;
    @JsonProperty("high")
    private BigDecimal highRate;
    @JsonProperty("low")
    private BigDecimal lowRate;
    private BigDecimal volume;
    @JsonProperty("last_trade_time")
    private LocalDateTime lastTradeTime;
    @JsonProperty("first_trade_time")
    private LocalDateTime firstTradeTime;
    @JsonProperty("time")
    private LocalDateTime candleOpenTime;

    @JsonProperty("pred_last_rate")
    private BigDecimal predLastRate;
    @JsonProperty("percent_change")
    private BigDecimal percentChange;
    @JsonProperty("value_change")
    private BigDecimal valueChange;
    @JsonProperty("currency_volume")
    private BigDecimal currencyVolume;
    @JsonProperty("highest_bid")
    private BigDecimal highestBid;
    @JsonProperty("lowest_ask")
    private BigDecimal lowestAsk;

    public static CandleModel empty(String pairName, BigDecimal closeRate, LocalDateTime candleOpenTime) {
        return CandleModel.builder()
                .pairName(pairName)
                .firstTradeTime(null)
                .lastTradeTime(null)
                .openRate(closeRate)
                .closeRate(closeRate)
                .highRate(closeRate)
                .lowRate(closeRate)
                .volume(BigDecimal.ZERO)
                .candleOpenTime(candleOpenTime)
                .predLastRate(closeRate)
                .currencyVolume(BigDecimal.ZERO)
                .highestBid(closeRate)
                .lowestAsk(closeRate)
                .build();
    }
}