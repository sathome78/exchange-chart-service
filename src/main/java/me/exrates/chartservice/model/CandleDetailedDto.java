package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class CandleDetailedDto {

    private String pairName;
    private String backDealInterval;
    private CandleDto candleDto;
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime lastDealTime;

    public CandleDetailedDto(String pairName, BackDealInterval backDealInterval, CandleDto candleDto, LocalDateTime lastDealTime) {
        this.pairName = pairName;
        this.backDealInterval = backDealInterval.getInterval();
        this.candleDto = candleDto;
        this.lastDealTime = lastDealTime;
    }
}
