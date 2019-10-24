package me.exrates.chartservice.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;
import me.exrates.chartservice.utils.TimeUtil;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class CandleDetailedDto {

    private String pairName;
    private String resolution;
    private CandleDto candleDto;
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime lastDealTime;

    public CandleDetailedDto(String pairName, BackDealInterval interval, CandleDto candleDto, LocalDateTime lastDealTime) {
        this.pairName = pairName;
        this.resolution = TimeUtil.convertToResolution(interval);
        this.candleDto = candleDto;
        this.lastDealTime = lastDealTime;
    }
}