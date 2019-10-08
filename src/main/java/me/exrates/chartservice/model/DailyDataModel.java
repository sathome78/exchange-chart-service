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
public class DailyDataModel {

    @JsonProperty("time")
    private LocalDateTime candleOpenTime;
    @JsonProperty("highest_bid")
    private BigDecimal highestBid;
    @JsonProperty("lowest_ask")
    private BigDecimal lowestAsk;
}