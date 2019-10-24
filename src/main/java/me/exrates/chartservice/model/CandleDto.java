package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;

import java.math.BigDecimal;
import java.time.LocalDateTime;

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
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime time;

    public static CandleDto toDto(CandleModel candleModel) {
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