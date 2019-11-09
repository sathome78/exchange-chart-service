package me.exrates.chartservice.converters;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.OrderDataDto;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class CandleDataConverter {

    /**
     * @param models   - list of candles for aggregating to backDealInterval
     * @param interval - interval for aggregating candles
     * @return unsorted list of candles, aggregated to specified backDealInterval
     */
    public static List<CandleModel> convertByInterval(List<CandleModel> models, BackDealInterval interval) {
        Map<LocalDateTime, List<CandleModel>> mapOfModels = models.stream()
                .collect(Collectors.groupingBy(model -> getNearestBackTimeForBackdealInterval(model.getCandleOpenTime(), interval)));

        return mapOfModels
                .entrySet().stream()
                .map(entry -> {
                    List<CandleModel> groupedCandles = entry.getValue();

                    groupedCandles.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

                    CandleModel firstCandle = groupedCandles.get(0);
                    final BigDecimal openRate = firstCandle.getOpenRate();
                    final LocalDateTime firstTradeTime = firstCandle.getFirstTradeTime();

                    CandleModel lastCandle = groupedCandles.get(groupedCandles.size() - 1);
                    final BigDecimal closeRate = lastCandle.getCloseRate();
                    final LocalDateTime lastTradeTime = lastCandle.getLastTradeTime();
                    final BigDecimal predLastRate = lastCandle.getPredLastRate();

                    final BigDecimal volume = groupedCandles.stream()
                            .map(CandleModel::getVolume)
                            .reduce(BigDecimal::add)
                            .orElse(BigDecimal.ZERO);
                    final BigDecimal currencyVolume = groupedCandles.stream()
                            .map(CandleModel::getCurrencyVolume)
                            .reduce(BigDecimal::add)
                            .orElse(BigDecimal.ZERO);

                    final BigDecimal highRate = groupedCandles.stream()
                            .map(CandleModel::getHighRate)
                            .max(Comparator.naturalOrder())
                            .orElse(BigDecimal.ZERO);
                    final BigDecimal lowRate = groupedCandles.stream()
                            .map(CandleModel::getLowRate)
                            .min(Comparator.naturalOrder())
                            .orElse(BigDecimal.ZERO);

                    return CandleModel.builder()
                            .pairName(firstCandle.getPairName())
                            .firstTradeTime(firstTradeTime)
                            .lastTradeTime(lastTradeTime)
                            .candleOpenTime(entry.getKey())
                            .openRate(openRate)
                            .closeRate(closeRate)
                            .volume(volume)
                            .highRate(highRate)
                            .lowRate(lowRate)
                            .predLastRate(predLastRate)
                            .percentChange(getPercentChange(closeRate, openRate))
                            .valueChange(getValueChange(closeRate, openRate))
                            .currencyVolume(currencyVolume)
                            .build();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static CandleModel reduceToCandle(List<OrderDataDto> tradeData) {
        if (CollectionUtils.isEmpty(tradeData)) {
            return null;
        }

        tradeData.sort(Comparator.comparing(OrderDataDto::getTradeDate));

        OrderDataDto firstTrade = tradeData.get(0);
        final BigDecimal openRate = firstTrade.getExrate();
        final LocalDateTime firstTradeTime = firstTrade.getTradeDate();

        OrderDataDto lastTrade = tradeData.get(tradeData.size() - 1);
        final BigDecimal closeRate = lastTrade.getExrate();
        final LocalDateTime lastTradeTime = lastTrade.getTradeDate();

        OrderDataDto predLastTrade = (tradeData.size() > 2)
                ? tradeData.get(tradeData.size() - 2)
                : tradeData.get(0);
        final BigDecimal predLastRate = predLastTrade.getExrate();

        final BigDecimal volume = tradeData.stream()
                .map(OrderDataDto::getAmountBase)
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);
        final BigDecimal currencyVolume = tradeData.stream()
                .map(OrderDataDto::getAmountConvert)
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);

        final BigDecimal highRate = tradeData.stream()
                .map(OrderDataDto::getExrate)
                .max(Comparator.naturalOrder())
                .orElse(BigDecimal.ZERO);
        final BigDecimal lowRate = tradeData.stream()
                .map(OrderDataDto::getExrate)
                .min(Comparator.naturalOrder())
                .orElse(BigDecimal.ZERO);

        return CandleModel.builder()
                .pairName(firstTrade.getCurrencyPairName())
                .firstTradeTime(firstTradeTime)
                .lastTradeTime(lastTradeTime)
                .candleOpenTime(TimeUtil.getNearestTimeBeforeForMinInterval(firstTradeTime))
                .openRate(openRate)
                .closeRate(closeRate)
                .volume(volume)
                .highRate(highRate)
                .lowRate(lowRate)
                .predLastRate(predLastRate)
                .percentChange(getPercentChange(closeRate, openRate))
                .valueChange(getValueChange(closeRate, openRate))
                .currencyVolume(currencyVolume)
                .build();
    }

    public static CoinmarketcapApiDto reduceToCoinmarketcapData(List<CandleModel> models, CurrencyPairDto currencyPairDto) {
        if (CollectionUtils.isEmpty(models)) {
            return null;
        }

        models.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        CandleModel firstCandle = models.get(0);
        final BigDecimal openRate = firstCandle.getOpenRate();

        CandleModel lastCandle = models.get(models.size() - 1);
        final BigDecimal closeRate = lastCandle.getCloseRate();

        final BigDecimal volume = models.stream()
                .map(CandleModel::getVolume)
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);
        final BigDecimal currencyVolume = models.stream()
                .map(CandleModel::getCurrencyVolume)
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);

        final BigDecimal highRate = models.stream()
                .map(CandleModel::getHighRate)
                .max(Comparator.naturalOrder())
                .orElse(BigDecimal.ZERO);
        final BigDecimal lowRate = models.stream()
                .map(CandleModel::getLowRate)
                .min(Comparator.naturalOrder())
                .orElse(BigDecimal.ZERO);

        return CoinmarketcapApiDto.builder()
                .currencyPairId(currencyPairDto.getId())
                .currencyPairName(currencyPairDto.getName())
                .first(openRate)
                .last(closeRate)
                .baseVolume(volume)
                .quoteVolume(currencyVolume)
                .high24hr(highRate)
                .low24hr(lowRate)
                .isFrozen(currencyPairDto.isHidden() ? 1 : 0)
                .percentChange(getPercentChange(closeRate, openRate))
                .valueChange(getValueChange(closeRate, openRate))
                .build();
    }

    public static void merge(CandleModel cachedModel, @NotNull CandleModel newModel) {
        boolean leftStartFirst = cachedModel.getFirstTradeTime().isBefore(newModel.getFirstTradeTime());
        boolean leftEndLast = cachedModel.getLastTradeTime().isAfter(newModel.getLastTradeTime());

        final BigDecimal closeRate = leftEndLast ? cachedModel.getCloseRate() : newModel.getCloseRate();
        final BigDecimal predLastRate = leftEndLast ? newModel.getCloseRate() : cachedModel.getCloseRate();

        cachedModel.setFirstTradeTime(leftStartFirst ? cachedModel.getFirstTradeTime() : newModel.getFirstTradeTime());
        cachedModel.setLastTradeTime(leftEndLast ? cachedModel.getLastTradeTime() : newModel.getLastTradeTime());
        cachedModel.setOpenRate(leftStartFirst ? cachedModel.getOpenRate() : newModel.getOpenRate());
        cachedModel.setCloseRate(closeRate);
        cachedModel.setHighRate(cachedModel.getHighRate().max(newModel.getHighRate()));
        cachedModel.setLowRate(cachedModel.getLowRate().min(newModel.getLowRate()));
        cachedModel.setVolume(cachedModel.getVolume().add(newModel.getVolume()));
        cachedModel.setPredLastRate(predLastRate);
        cachedModel.setPercentChange(getPercentChange(cachedModel.getCloseRate(), cachedModel.getOpenRate()));
        cachedModel.setValueChange(getValueChange(cachedModel.getCloseRate(), cachedModel.getOpenRate()));
        cachedModel.setCurrencyVolume(cachedModel.getCurrencyVolume().add(newModel.getCurrencyVolume()));
    }

    private static BigDecimal getPercentChange(BigDecimal closeRate, BigDecimal openRate) {
        BigDecimal percentChange = BigDecimal.ZERO;
        if (closeRate.compareTo(BigDecimal.ZERO) > 0 && openRate.compareTo(BigDecimal.ZERO) > 0) {
            percentChange = closeRate.divide(openRate, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100)).subtract(BigDecimal.valueOf(100));
        }
        if (closeRate.compareTo(BigDecimal.ZERO) > 0 && openRate.compareTo(BigDecimal.ZERO) == 0) {
            percentChange = new BigDecimal(100);
        }
        return percentChange;
    }

    private static BigDecimal getValueChange(BigDecimal closeRate, BigDecimal openRate) {
        BigDecimal valueChange = BigDecimal.ZERO;
        if (closeRate.compareTo(BigDecimal.ZERO) > 0 && openRate.compareTo(BigDecimal.ZERO) > 0) {
            valueChange = closeRate.subtract(openRate);
        }
        if (closeRate.compareTo(BigDecimal.ZERO) > 0 && openRate.compareTo(BigDecimal.ZERO) == 0) {
            valueChange = closeRate;
        }
        return valueChange;
    }

    public static BigDecimal getCurrentHighestBid(BigDecimal savedHighestBid, BigDecimal newHighestBid) {
        BigDecimal highestBid;
        if (Objects.isNull(savedHighestBid) && Objects.isNull(newHighestBid)) {
            highestBid = null;
        } else if (Objects.isNull(savedHighestBid)) {
            highestBid = newHighestBid;
        } else if (Objects.isNull(newHighestBid)) {
            highestBid = savedHighestBid;
        } else {
            highestBid = savedHighestBid.max(newHighestBid);
        }
        return highestBid;
    }

    public static BigDecimal getCurrentLowestAsk(BigDecimal savedLowestAsk, BigDecimal newLowestAsk) {
        BigDecimal lowestAsk;
        if (Objects.isNull(savedLowestAsk) && Objects.isNull(newLowestAsk)) {
            lowestAsk = null;
        } else if (Objects.isNull(savedLowestAsk)) {
            lowestAsk = newLowestAsk;
        } else if (Objects.isNull(newLowestAsk)) {
            lowestAsk = savedLowestAsk;
        } else {
            lowestAsk = savedLowestAsk.min(newLowestAsk);
        }
        return lowestAsk;
    }

    public static List<CandleModel> convert(List<OrderDto> orders) {
        Map<LocalDateTime, List<OrderDataDto>> groupedByTradeDate = orders.stream()
                .map(OrderDataDto::new)
                .collect(Collectors.groupingBy(tradeData -> TimeUtil.getNearestTimeBeforeForMinInterval(tradeData.getTradeDate())));

        return groupedByTradeDate.entrySet().stream()
                .map(entry -> CandleDataConverter.reduceToCandle(entry.getValue()))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(CandleModel::getCandleOpenTime))
                .collect(Collectors.toList());
    }

    public static List<CandleModel> filterModelsByRange(List<CandleModel> models, LocalDateTime from, LocalDateTime to) {
        return models.stream()
                .filter(model -> (model.getCandleOpenTime().isAfter(from) || model.getCandleOpenTime().isEqual(from)) &&
                        (model.getCandleOpenTime().isBefore(to) || model.getCandleOpenTime().isEqual(to)))
                .collect(Collectors.toList());
    }
}