package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import me.exrates.chartservice.model.serializers.CurrencyPairDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;
import me.exrates.chartservice.model.OrderDto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter@Setter
@NoArgsConstructor
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


    public TradeDataDto(OrderDto order) {
        this.orderId = order.getId();
        this.pairName = order.getCurrencyPairName();
        this.exrate = order.getExRate();
        this.amountBase = order.getAmountBase();
        this.amountConvert = order.getAmountConvert();
        this.tradeDate = order.getDateAcception();
    }

    public TradeDataDto(String pairName, BigDecimal exrate, BigDecimal amountBase, LocalDateTime tradeDate) {
        this.pairName = pairName;
        this.exrate = exrate;
        this.amountBase = amountBase;
        this.tradeDate = tradeDate;
    }
}
