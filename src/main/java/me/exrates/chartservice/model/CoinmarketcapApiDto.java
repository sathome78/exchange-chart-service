package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CoinmarketcapApiDto {

    @JsonProperty("currency_pair_id")
    private Integer currencyPairId;
    @JsonProperty("currency_pair_name")
    private String currencyPairName;
    private BigDecimal first;
    private BigDecimal last;
    @JsonProperty("base_volume")
    private BigDecimal baseVolume;
    @JsonProperty("quote_volume")
    private BigDecimal quoteVolume;
    @JsonProperty("high_24hr")
    private BigDecimal high24hr;
    @JsonProperty("low_24hr")
    private BigDecimal low24hr;
    @JsonProperty("highest_bid")
    private BigDecimal highestBid;
    @JsonProperty("lowest_ask")
    private BigDecimal lowestAsk;
    @JsonProperty("is_frozen")
    private Integer isFrozen;
    @JsonProperty("percent_change")
    private BigDecimal percentChange;
    @JsonProperty("value_change")
    private BigDecimal valueChange;
}