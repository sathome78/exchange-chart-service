package me.exrates.chartservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto implements Serializable {

    private int id;
    private String currencyPairName;
    private BigDecimal exRate;
    private BigDecimal amountBase;
    private BigDecimal amountConvert;
    private LocalDateTime dateAcception;
}