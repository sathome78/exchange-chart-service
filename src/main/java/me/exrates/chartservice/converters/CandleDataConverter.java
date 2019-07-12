package me.exrates.chartservice.converters;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.TradeDataDto;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;
import static me.exrates.chartservice.utils.TimeUtil.getNearestTimeBeforeForMinInterval;

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
                .map(entry -> {
                    List<CandleModel> groupedCandles = entry.getValue();
                    groupedCandles.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

                    return groupedCandles.stream()
                            .reduce((left, right) -> CandleModel.builder()
                                    .highRate(left.getHighRate().max(right.getHighRate()))
                                    .lowRate(left.getLowRate().min(right.getLowRate()))
                                    .volume(left.getVolume().add(right.getVolume()))
                                    .build())
                            .map(candleModel -> candleModel.toBuilder()
                                    .openRate(groupedCandles.get(0).getOpenRate())
                                    .closeRate(groupedCandles.get(groupedCandles.size() - 1).getCloseRate())
                                    .candleOpenTime(entry.getKey())
                                    .build())
                            .orElse(null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static CandleModel reduceToCandle(List<TradeDataDto> tradeData) {
        if (CollectionUtils.isEmpty(tradeData)) {
            return null;
        }
        tradeData.sort(Comparator.comparing(TradeDataDto::getTradeDate));
        TradeDataDto firstTrade = tradeData.get(0);
        TradeDataDto lastTrade = tradeData.get(tradeData.size() - 1);
        BigDecimal volume = BigDecimal.valueOf(tradeData.parallelStream().mapToDouble(p->p.getAmountBase().doubleValue()).sum());
        DoubleSummaryStatistics summaryStatistics = tradeData.parallelStream().mapToDouble(p->p.getExrate().doubleValue()).summaryStatistics();
        return CandleModel.builder()
                .firstTradeTime(tradeData.get(0).getTradeDate())
                .lastTradeTime(tradeData.get(tradeData.size() - 1).getTradeDate())
                .candleOpenTime(getNearestTimeBeforeForMinInterval(tradeData.get(0).getTradeDate()))
                .openRate(firstTrade.getExrate())
                .closeRate(lastTrade.getExrate())
                .volume(volume)
                .highRate(new BigDecimal(summaryStatistics.getMax()))
                .lowRate(new BigDecimal(summaryStatistics.getMin()))
                .build();
    }

    public static CandleModel merge(CandleModel cachedCandle, @NotNull CandleModel newCandle) {
        if (cachedCandle == null) {
            return newCandle;
        }
        boolean leftStartFirst = cachedCandle.getFirstTradeTime().isBefore(newCandle.getFirstTradeTime());
        boolean leftEndLast = cachedCandle.getLastTradeTime().isAfter(newCandle.getLastTradeTime());
        return CandleModel.builder()
                .firstTradeTime(leftStartFirst ? cachedCandle.getFirstTradeTime() : newCandle.getFirstTradeTime())
                .lastTradeTime(leftEndLast ? cachedCandle.getLastTradeTime() : newCandle.getLastTradeTime())
                .candleOpenTime(cachedCandle.getCandleOpenTime())
                .openRate(leftStartFirst ? cachedCandle.getOpenRate() : newCandle.getOpenRate())
                .closeRate(leftEndLast ? cachedCandle.getCloseRate() : newCandle.getCloseRate())
                .volume(cachedCandle.getVolume().add(newCandle.getVolume()))
                .highRate(cachedCandle.getHighRate().max(newCandle.getHighRate()))
                .lowRate(cachedCandle.getLowRate().min(newCandle.getLowRate()))
                .build();

    }
}
