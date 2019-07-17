package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;
import lombok.Data;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
public class CandleDto {

    private BigDecimal open;
    private BigDecimal close;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal volume;
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime time;

    public static CandleDto fromCandleModel(CandleModel candleModel) {
        return CandleDto.builder()
                .open(candleModel.getOpenRate())
                .close(candleModel.getCloseRate())
                .high(candleModel.getHighRate())
                .low(candleModel.getLowRate())
                .volume(candleModel.getVolume())
                .time(candleModel.getCandleOpenTime())
                .build();
    }

}