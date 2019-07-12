package me.exrates.chartservice.converters;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class CandleDataConverter {

    /**
     * @param candleModels - list of candles for aggregating to backDealInterval
     * @param interval     - interval for aggregating candles
     * @return unsorted list of candles, aggregated to specified backDealInterval
     */
    public static List<CandleModel> convertByInterval(List<CandleModel> candleModels, BackDealInterval interval) {
        return candleModels.stream()
                .collect(Collectors.groupingBy(p -> getNearestBackTimeForBackdealInterval(p.getCandleOpenTime(), interval)))
                .entrySet().stream()
                .map(x -> {
                    List<CandleModel> groupedCandles = x.getValue();
                    groupedCandles.sort(Comparator.comparing(CandleModel::getCandleOpenTime));
                    CandleModel model = groupedCandles.stream()
                            .reduce(null, (left, right) -> new CandleModel(left.getVolume().add(right.getVolume()),
                                    left.getLowRate().min(right.getLowRate()),
                                    left.getHighRate().max(right.getHighRate())));
                    model.setOpenRate(groupedCandles.get(0).getOpenRate());
                    model.setCloseRate(groupedCandles.get(groupedCandles.size() - 1).getCloseRate());
                    model.setCandleOpenTime(x.getKey());
                    return model;
                })
                .collect(Collectors.toList());
    }
}