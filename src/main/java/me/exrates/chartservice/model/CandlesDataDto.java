package me.exrates.chartservice.model;

import lombok.Data;

import java.util.List;

@Data
public class CandlesDataDto {

    private List<CandleModel> candleModels;
    private String pairName;
    private BackDealInterval interval;
}
