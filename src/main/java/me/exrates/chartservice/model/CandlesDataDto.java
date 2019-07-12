package me.exrates.chartservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class CandlesDataDto {

    private List<CandleModel> candleModels;
    private String pairName;
    private BackDealInterval interval;
}