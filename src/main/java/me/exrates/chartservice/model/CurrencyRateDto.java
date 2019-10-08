package me.exrates.chartservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CurrencyRateDto {

    private String currencyName;
    private BigDecimal usdRate;
    private BigDecimal btcRate;
}