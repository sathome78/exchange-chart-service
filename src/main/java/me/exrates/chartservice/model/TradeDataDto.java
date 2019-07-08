package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import me.exrates.chartservice.model.serializers.CurrencyPairDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class TradeDataDto {

    private int orderId;
    @JsonDeserialize(using = CurrencyPairDeserializer.class)
    private String pairName;
    private BigDecimal exrate;
    private BigDecimal amountBase;
    private BigDecimal amountConvert;
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime tradeDate;
}
