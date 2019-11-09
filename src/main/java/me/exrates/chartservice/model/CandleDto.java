package me.exrates.chartservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.ZoneOffset;

@Getter
@Setter
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor
public class CandleDto {

    private BigDecimal open;
    private BigDecimal close;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal volume;
    private Long time;

    public static CandleDto toDto(CandleModel candleModel) {
        return CandleDto.builder()
                .open(candleModel.getOpenRate())
                .close(candleModel.getCloseRate())
                .high(candleModel.getHighRate())
                .low(candleModel.getLowRate())
                .volume(candleModel.getVolume())
                .time(candleModel.getCandleOpenTime().toEpochSecond(ZoneOffset.UTC))
                .build();
    }
}