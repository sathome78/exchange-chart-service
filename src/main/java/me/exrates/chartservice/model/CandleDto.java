package me.exrates.chartservice.model;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.ZoneOffset;

@Data
@Builder
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