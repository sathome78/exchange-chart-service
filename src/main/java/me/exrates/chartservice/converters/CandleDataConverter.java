package me.exrates.chartservice.converters;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class CandleDataConverter {

    /**
     * @param models   - list of candles for aggregating to backDealInterval
     * @param interval - interval for aggregating candles
     * @return unsorted list of candles, aggregated to specified backDealInterval
     */
    public static List<CandleModel> convertByInterval(List<CandleModel> models, BackDealInterval interval) {
        return models.stream()
                .sorted(Comparator.comparing(CandleModel::getCandleOpenTime))
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
                            .map(model -> model.toBuilder()
                                    .firstTradeTime(groupedCandles.get(0).getFirstTradeTime())
                                    .lastTradeTime(groupedCandles.get(groupedCandles.size() - 1).getLastTradeTime())
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

        BigDecimal volume = tradeData.stream()
                .map(TradeDataDto::getAmountBase)
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);
        DoubleSummaryStatistics summaryStatistics = tradeData.stream()
                .mapToDouble(p -> p.getExrate().doubleValue()).summaryStatistics();

        return CandleModel.builder()
                .pairName(firstTrade.getPairName())
                .firstTradeTime(firstTrade.getTradeDate())
                .lastTradeTime(lastTrade.getTradeDate())
                .candleOpenTime(TimeUtil.getNearestTimeBeforeForMinInterval(firstTrade.getTradeDate()))
                .openRate(firstTrade.getExrate())
                .closeRate(lastTrade.getExrate())
                .volume(volume)
                .highRate(new BigDecimal(summaryStatistics.getMax()))
                .lowRate(new BigDecimal(summaryStatistics.getMin()))
                .build();
    }

    public static void merge(CandleModel cachedModel, @NotNull CandleModel newModel) {
        boolean leftStartFirst = cachedModel.getFirstTradeTime().isBefore(newModel.getFirstTradeTime());
        boolean leftEndLast = cachedModel.getLastTradeTime().isAfter(newModel.getLastTradeTime());

        cachedModel.setFirstTradeTime(leftStartFirst ? cachedModel.getFirstTradeTime() : newModel.getFirstTradeTime());
        cachedModel.setLastTradeTime(leftEndLast ? cachedModel.getLastTradeTime() : newModel.getLastTradeTime());
        cachedModel.setOpenRate(leftStartFirst ? cachedModel.getOpenRate() : newModel.getOpenRate());
        cachedModel.setCloseRate(leftEndLast ? cachedModel.getCloseRate() : newModel.getCloseRate());
        cachedModel.setHighRate(cachedModel.getHighRate().max(newModel.getHighRate()));
        cachedModel.setLowRate(cachedModel.getLowRate().min(newModel.getLowRate()));
        cachedModel.setVolume(cachedModel.getVolume().add(newModel.getVolume()));
    }

    public static List<CandleModel> convert(List<OrderDto> orders) {
        Map<LocalDateTime, List<TradeDataDto>> groupedByTradeDate = orders.stream()
                .map(TradeDataDto::new)
                .collect(Collectors.groupingBy(tradeData -> TimeUtil.getNearestTimeBeforeForMinInterval(tradeData.getTradeDate())));

        return groupedByTradeDate.entrySet().stream()
                .map(entry -> CandleDataConverter.reduceToCandle(entry.getValue()))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(CandleModel::getCandleOpenTime))
                .collect(Collectors.toList());
    }

    public static List<CandleModel> fillGaps(List<CandleModel> models, String pairName, BackDealInterval interval) {
        CandleModel initialCandle = models.get(0);
        LocalDateTime from = initialCandle.getCandleOpenTime();
        LocalDateTime to = models.get(models.size() - 1).getCandleOpenTime();

        final Map<LocalDateTime, CandleModel> modelsMap = models.stream()
                .collect(Collectors.toMap(CandleModel::getCandleOpenTime, Function.identity()));

        final int minutes = TimeUtil.convertToMinutes(interval);

        while (from.isBefore(to)) {
            from = from.plusMinutes(minutes);

            CandleModel model = modelsMap.get(from);

            if (isNull(model)) {
                models.add(CandleModel.empty(pairName, initialCandle.getCloseRate(), from));
            } else {
                initialCandle = model;
            }
        }
        return models.stream()
                .sorted(Comparator.comparing(CandleModel::getCandleOpenTime))
                .collect(Collectors.toList());
    }

    public static void fixOpenRate(List<CandleModel> models) {
        models.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        IntStream.range(1, models.size())
                .forEach(i -> models.get(i).setOpenRate(models.get(i - 1).getCloseRate()));
    }
}