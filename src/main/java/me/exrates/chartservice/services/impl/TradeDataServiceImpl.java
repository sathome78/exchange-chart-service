package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDataDto;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.StompMessengerService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.configuration.CommonConfiguration.TRADE_SYNC;
import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;

@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final StompMessengerService messengerService;
    private final XSync<String> xSync;

    private long candlesToStoreInCache;

    private List<BackDealInterval> supportedIntervals;

    @Autowired
    public TradeDataServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                RedisProcessingService redisProcessingService,
                                StompMessengerService messengerService,
                                @Qualifier(TRADE_SYNC) XSync<String> xSync,
                                @Value("${candles.store-in-cache:300}") long candlesToStoreInCache,
                                @Qualifier(ALL_SUPPORTED_INTERVALS_LIST) List<BackDealInterval> supportedIntervals) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.messengerService = messengerService;
        this.xSync = xSync;
        this.candlesToStoreInCache = candlesToStoreInCache;
        this.supportedIntervals = supportedIntervals;
    }

    @Override
    public CandleModel getCandleForCurrentTime(String pairName, BackDealInterval interval) {
        return getCandle(pairName, LocalDateTime.now(), interval);
    }

    private CandleModel getCandle(String pairName, LocalDateTime dateTime, BackDealInterval interval) {
        LocalDateTime candleTime = getNearestBackTimeForBackdealInterval(dateTime, interval);

        final String key = RedisGeneratorUtil.generateKey(candleTime.toLocalDate());
        final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

        List<CandleModel> models = redisProcessingService.get(key, hashKey, interval);
        if (CollectionUtils.isEmpty(models)) {
            return getEmptyModel(pairName, candleTime, interval);
        }
        return models.stream()
                .filter(model -> model.getCandleOpenTime().isEqual(candleTime))
                .peek(model -> {
                    final CandleModel previousModel = getPreviousCandle(pairName, candleTime, interval);
                    if (Objects.nonNull(previousModel)) {
                        model.setOpenRate(previousModel.getCloseRate());
                    }
                })
                .findFirst()
                .orElse(getEmptyModel(pairName, candleTime, interval));
    }

    private CandleModel getEmptyModel(String pairName, LocalDateTime candleTime, BackDealInterval interval) {
        final CandleModel previousModel = getPreviousCandle(pairName, candleTime, interval);

        if (Objects.isNull(previousModel)) {
            return null;
        }
        return CandleModel.empty(pairName, previousModel.getCloseRate(), candleTime);
    }

    @Override
    public List<CandleModel> getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        if (isNull(from) || isNull(to) || from.isAfter(to)) {
            return Collections.emptyList();
        }

        LocalDate fromDate = from.toLocalDate();
        LocalDate toDate = to.toLocalDate();

        final LocalDate boundaryDate = TimeUtil.getBoundaryTime(candlesToStoreInCache, interval);

        List<CandleModel> models;
        if (toDate.isBefore(boundaryDate)) {
            models = getCandlesFromElasticAndAggregateToInterval(pairName, fromDate, toDate, interval);
        } else if (fromDate.isAfter(boundaryDate)) {
            models = getCandlesFromRedis(pairName, fromDate, toDate, interval);
        } else {
            models = Stream.of(
                    getCandlesFromElasticAndAggregateToInterval(pairName, fromDate, boundaryDate.minusDays(1), interval),
                    getCandlesFromRedis(pairName, boundaryDate, toDate, interval))
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(Collectors.toList());
        }

        if (CollectionUtils.isEmpty(models)) {
            return models;
        }

        fixOpenRate(models, pairName, interval);

        models = CandleDataConverter.filterModelsByRange(
                models,
                TimeUtil.getNearestBackTimeForBackdealInterval(from, interval),
                TimeUtil.getNearestBackTimeForBackdealInterval(to, interval));

        return fillGaps(models, pairName, to, interval);
    }

    private void fixOpenRate(List<CandleModel> models, String pairName, BackDealInterval interval) {
        models.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        final CandleModel previousModel = getPreviousCandle(pairName, models.get(0).getCandleOpenTime(), interval);
        if (Objects.nonNull(previousModel)) {
            models.get(0).setOpenRate(previousModel.getCloseRate());
        }

        IntStream.range(1, models.size())
                .forEach(i -> models.get(i).setOpenRate(models.get(i - 1).getCloseRate()));
    }

    private List<CandleModel> fillGaps(List<CandleModel> models, String pairName, LocalDateTime to, BackDealInterval interval) {
        to = TimeUtil.getNearestBackTimeForBackdealInterval(to, interval);

        final CandleModel previousModel = getPreviousCandle(pairName, to, interval);
        if (Objects.isNull(previousModel)) {
            return models;
        }

        final int minutes = TimeUtil.convertToMinutes(interval);

        LocalDateTime from = previousModel.getCandleOpenTime().plusMinutes(minutes);

        while (from.isBefore(to) || from.isEqual(to)) {
            LocalDateTime finalFrom = from;
            boolean notPresent = models.stream().noneMatch(model -> model.getCandleOpenTime().isEqual(finalFrom));

            if (notPresent) {
                models.add(CandleModel.empty(pairName, previousModel.getCloseRate(), from));
            }

            from = from.plusMinutes(minutes);
        }

        models.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        return models;
    }

    @Override
    public LocalDateTime getLastCandleTimeBeforeDate(String pairName, LocalDateTime candleDateTime, BackDealInterval interval) {
        if (isNull(candleDateTime)) {
            return null;
        }

        candleDateTime = getNearestBackTimeForBackdealInterval(candleDateTime, interval);

        LocalDateTime boundaryTime = TimeUtil.getBoundaryTime(candlesToStoreInCache, interval).atTime(0, 0);

        if (candleDateTime.isAfter(boundaryTime)) {
            final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

            return redisProcessingService.getLastCandleTimeBeforeDate(candleDateTime, boundaryTime, hashKey, interval);
        } else {
            final String id = ElasticsearchGeneratorUtil.generateId(pairName);

            boundaryTime = redisProcessingService.getFirstInitializedCandleTimeFromHistory(id);
            if (Objects.isNull(boundaryTime)) {
                return null;
            }
            return elasticsearchProcessingService.getLastCandleTimeBeforeDate(candleDateTime, boundaryTime, id);
        }
    }

    @Override
    public void handleReceivedTrades(String pairName, List<OrderDataDto> ordersData) {
        ordersData.stream()
                .collect(Collectors.groupingBy(dto -> TimeUtil.getNearestTimeBeforeForMinInterval(dto.getTradeDate())))
                .values()
                .forEach(trades -> groupTradesAndSave(pairName, trades));
    }

    private void groupTradesAndSave(String pairName, List<OrderDataDto> ordersData) {
        xSync.execute(pairName, () -> {
            CandleModel newModel = CandleDataConverter.reduceToCandle(ordersData);
            if (isNull(newModel)) {
                return;
            }

            supportedIntervals.forEach(interval -> {
                final LocalDateTime candleDateTime = TimeUtil.getNearestBackTimeForBackdealInterval(newModel.getCandleOpenTime(), interval);
                newModel.setCandleOpenTime(candleDateTime);

                final String key = RedisGeneratorUtil.generateKey(candleDateTime.toLocalDate());
                final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

                List<CandleModel> cachedModels = redisProcessingService.get(key, hashKey, interval);

                CandleModel finalModel = newModel;
                if (!CollectionUtils.isEmpty(cachedModels)) {
                    cachedModels = new ArrayList<>(cachedModels);

                    CandleModel cachedModel = cachedModels.stream()
                            .filter(model -> model.getCandleOpenTime().isEqual(candleDateTime))
                            .peek(model -> CandleDataConverter.merge(model, newModel))
                            .findFirst()
                            .orElse(null);

                    if (Objects.isNull(cachedModel)) {
                        cachedModels.add(newModel);
                    } else {
                        finalModel = cachedModel;
                    }
                } else {
                    cachedModels = Collections.singletonList(newModel);
                }

                redisProcessingService.insertOrUpdate(cachedModels, key, hashKey, interval);

                defineAndSaveLastInitializedCandleTime(hashKey, cachedModels);

                //send last candle data to the front end
                mapAndSendLastCandle(finalModel, interval);
            });
        });
    }

    private void mapAndSendLastCandle(CandleModel finalModel, BackDealInterval interval) {
        final String pairName = finalModel.getPairName();
        final LocalDateTime candleDateTime = finalModel.getCandleOpenTime();

        final CandleModel previousModel = getPreviousCandle(pairName, candleDateTime, interval);
        if (Objects.nonNull(previousModel)) {
            finalModel.setOpenRate(previousModel.getCloseRate());
        }
        messengerService.sendLastCandle(finalModel, pairName, interval);
    }

    @Override
    public void defineAndSaveLastInitializedCandleTime(String hashKey, List<CandleModel> models) {
        if (!CollectionUtils.isEmpty(models)) {
            models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .max(LocalDateTime::compareTo)
                    .ifPresent(dateTime -> {
                        LocalDateTime lastInitializedCandleTime = redisProcessingService.getLastInitializedCandleTimeFromCache(hashKey);

                        if (Objects.isNull(lastInitializedCandleTime) || lastInitializedCandleTime.isBefore(dateTime)) {
                            redisProcessingService.insertLastInitializedCandleTimeToCache(hashKey, dateTime);
                        }
                    });
        }
    }

    @Override
    public void defineAndSaveFirstInitializedCandleTime(String hashKey, List<CandleModel> models) {
        if (!CollectionUtils.isEmpty(models)) {
            models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .min(LocalDateTime::compareTo)
                    .ifPresent(dateTime -> {
                        LocalDateTime firstInitializedCandleTime = redisProcessingService.getFirstInitializedCandleTimeFromHistory(hashKey);

                        if (Objects.isNull(firstInitializedCandleTime) || firstInitializedCandleTime.isAfter(dateTime)) {
                            redisProcessingService.insertFirstInitializedCandleTimeToHistory(hashKey, dateTime);
                        }
                    });
        }
    }

    private CandleModel getPreviousCandle(String pairName, LocalDateTime candleDateTime, BackDealInterval interval) {
        LocalDateTime boundaryTime = TimeUtil.getBoundaryTime(candlesToStoreInCache, interval).atTime(0, 0);

        CandleModel previousModel;
        if (candleDateTime.isAfter(boundaryTime)) {
            final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

            LocalDateTime lastCandleTimeBeforeDate = redisProcessingService.getLastCandleTimeBeforeDate(candleDateTime, boundaryTime, hashKey, interval);
            if (Objects.isNull(lastCandleTimeBeforeDate)) {
                return null;
            }

            final String key = RedisGeneratorUtil.generateKey(lastCandleTimeBeforeDate.toLocalDate());

            List<CandleModel> models = redisProcessingService.get(key, hashKey, interval);
            if (CollectionUtils.isEmpty(models)) {
                return null;
            }
            previousModel = models.stream()
                    .filter(model -> model.getCandleOpenTime().isEqual(lastCandleTimeBeforeDate))
                    .findFirst()
                    .orElse(null);
        } else {
            final String id = ElasticsearchGeneratorUtil.generateId(pairName);

            boundaryTime = redisProcessingService.getFirstInitializedCandleTimeFromHistory(id);
            if (Objects.isNull(boundaryTime)) {
                return null;
            }

            LocalDateTime lastCandleTimeBeforeDate = elasticsearchProcessingService.getLastCandleTimeBeforeDate(candleDateTime, boundaryTime, id);
            if (Objects.isNull(lastCandleTimeBeforeDate)) {
                return null;
            }

            final String index = ElasticsearchGeneratorUtil.generateIndex(lastCandleTimeBeforeDate.toLocalDate());

            List<CandleModel> models = elasticsearchProcessingService.get(index, id);
            if (CollectionUtils.isEmpty(models)) {
                return null;
            }
            previousModel = models.stream()
                    .filter(model -> model.getCandleOpenTime().isEqual(lastCandleTimeBeforeDate))
                    .findFirst()
                    .orElse(null);
        }
        return previousModel;
    }

    private List<CandleModel> getCandlesFromElasticAndAggregateToInterval(String pairName, LocalDate fromDate, LocalDate toDate, BackDealInterval interval) {
        final String id = ElasticsearchGeneratorUtil.generateId(pairName);

        List<CandleModel> bufferedModels = new ArrayList<>();
        while (fromDate.isBefore(toDate) || fromDate.isEqual(toDate)) {
            final String index = ElasticsearchGeneratorUtil.generateIndex(fromDate);

            try {
                List<CandleModel> models = elasticsearchProcessingService.get(index, id);
                if (!CollectionUtils.isEmpty(models)) {
                    bufferedModels.addAll(models);
                }
            } catch (Exception ex) {
                log.error(ex);
            }

            fromDate = fromDate.plusDays(1);
        }
        return CandleDataConverter.convertByInterval(bufferedModels, interval);
    }

    private List<CandleModel> getCandlesFromRedis(String pairName, LocalDate fromDate, LocalDate toDate, BackDealInterval interval) {
        final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

        List<CandleModel> bufferedModels = new ArrayList<>();
        while (fromDate.isBefore(toDate) || fromDate.isEqual(toDate)) {
            final String key = RedisGeneratorUtil.generateKey(fromDate);

            try {
                List<CandleModel> models = redisProcessingService.get(key, hashKey, interval);
                if (!CollectionUtils.isEmpty(models)) {
                    bufferedModels.addAll(models);
                }
            } catch (Exception ex) {
                log.error(ex);
            }

            fromDate = fromDate.plusDays(1);
        }
        return bufferedModels;
    }
}