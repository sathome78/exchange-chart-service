package me.exrates.chartservice.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
public class CandleDetailedDto {

    private String pairName;
    private String backDealInterval;
    private CandleDto candleDto;

    public CandleDetailedDto(String pairName, BackDealInterval backDealInterval, CandleDto candleDto) {
        this.pairName = pairName;
        this.backDealInterval = backDealInterval.getInterval();
        this.candleDto = candleDto;
    }
}
