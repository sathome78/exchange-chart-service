package me.exrates.chartservice.converters;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

                    Optional<CandleModel> optional = groupedCandles.stream()
                            .reduce((left, right) -> {
                                BigDecimal high = left.getHighRate().max(right.getHighRate());
                                BigDecimal low = left.getLowRate().min(right.getLowRate());
                                BigDecimal volume = left.getVolume().add(right.getVolume());

                                return new CandleModel(high, low, volume);
                            });

                    if (optional.isPresent()) {
                        CandleModel model = optional.get();
                        model.setOpenRate(groupedCandles.get(0).getOpenRate());
                        model.setCloseRate(groupedCandles.get(groupedCandles.size() - 1).getCloseRate());
                        model.setCandleOpenTime(x.getKey());
                        return model;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}