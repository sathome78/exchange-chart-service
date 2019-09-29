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
public class CurrencyPairDto {

    private String name;
    private boolean hidden;
    private String matket;
    private BigDecimal scale;
    @JsonProperty("top_market")
    private boolean topMarket;
    @JsonProperty("top_market_volume")
    private BigDecimal topMarketVolume;
}