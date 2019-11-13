package me.exrates.chartservice.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.enums.IntervalType;

@Data
@NoArgsConstructor
public class BackDealInterval {

    private Integer intervalValue;
    private IntervalType intervalType;

    public BackDealInterval(Integer intervalValue, IntervalType intervalType) {
        this.intervalValue = intervalValue;
        this.intervalType = intervalType;
    }

    public String getInterval() {
        return intervalValue + " " + intervalType;
    }
}