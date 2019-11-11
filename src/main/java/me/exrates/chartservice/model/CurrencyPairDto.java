package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CurrencyPairDto {

    private int id;
    private String name;
    private boolean hidden;
    private String matket;
    private String type;
    private int scale;
    @JsonProperty("top_market")
    private boolean topMarket;
}