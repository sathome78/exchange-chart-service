package me.exrates.chartservice.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CandleDetailedDto {

    private String pairName;
    private BackDealInterval backDealInterval;
    private CandleDto candleDto;

    public CandleDetailedDto(String pairName, BackDealInterval backDealInterval, CandleDto candleDto) {
        this.pairName = pairName;
        this.backDealInterval = backDealInterval;
        this.candleDto = candleDto;
    }
}
