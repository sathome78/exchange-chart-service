package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Tolerate;
import me.exrates.chartservice.model.serializers.CurrencyPairDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;
import org.apache.commons.math3.random.RandomDataGenerator;

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

    @Tolerate
    public TradeDataDto(String pairName, BigDecimal exrate, BigDecimal amountBase, LocalDateTime tradeDate) {
        this.pairName = pairName;
        this.exrate = exrate;
        this.amountBase = amountBase;
        this.tradeDate = tradeDate;
    }

    public static TradeDataDto createTradeWithRandomTime(String currencyPair) {
        TradeDataDto tradeDataDto = new TradeDataDto();
        tradeDataDto.setPairName(currencyPair);
        tradeDataDto.setTradeDate(LocalDateTime.now().plusSeconds(new RandomDataGenerator().nextLong(0, 10000)));
        return tradeDataDto;
    }
}
