package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.configuration.CommonConfiguration.TRADE_SYNC;
import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;

@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final XSync<String> xSync;

    private long candlesToStoreInCache;

    private List<BackDealInterval> supportedIntervals;

    @Autowired
    public TradeDataServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                RedisProcessingService redisProcessingService,
                                @Qualifier(TRADE_SYNC) XSync<String> xSync,
                                @Value("${candles.store-in-cache:300}") long candlesToStoreInCache,
                                @Qualifier(ALL_SUPPORTED_INTERVALS_LIST) List<BackDealInterval> supportedIntervals) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
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
            return null;
        }
        return models.stream()
                .filter(model -> model.getCandleOpenTime().isEqual(candleTime))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<CandleModel> getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        if (isNull(from) || isNull(to) || from.isAfter(to)) {
            return Collections.emptyList();
        }

        LocalDate fromDate = from.toLocalDate();
        LocalDate toDate = to.toLocalDate();

        final LocalDate boundaryDate = getBoundaryTime(interval);

        List<CandleModel> models;
        if (toDate.isBefore(boundaryDate)) {
            models = getCandlesFromElasticAndAggregateToInterval(pairName, fromDate, toDate, interval);
        } else if (fromDate.isBefore(boundaryDate) && toDate.isAfter(boundaryDate)) {
            models = Stream.of(
                    getCandlesFromElasticAndAggregateToInterval(pairName, fromDate, boundaryDate.minusDays(1), interval),
                    getCandlesFromRedis(pairName, boundaryDate, toDate, interval))
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(Collectors.toList());
        } else {
            models = getCandlesFromRedis(pairName, fromDate, toDate, interval);
        }

        CandleDataConverter.fixOpenRate(models);

        return fillGaps(models, pairName, from, to, interval);
    }

    private List<CandleModel> fillGaps(List<CandleModel> models, String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        List<CandleModel> bufferedModels = new ArrayList<>(models);
        final int minutes = TimeUtil.convertToMinutes(interval);

        CandleModel initialCandle;

        if (CollectionUtils.isEmpty(bufferedModels)) {
            CandleModel previousCandle = getPreviousCandle(pairName, from, interval);

            initialCandle = nonNull(previousCandle) ? previousCandle : CandleModel.empty(pairName, BigDecimal.ZERO, null);

            while (from.isBefore(to)) {
                bufferedModels.add(CandleModel.empty(pairName, initialCandle.getCloseRate(), from));

                from = from.plusMinutes(minutes);
            }
            bufferedModels.add(CandleModel.empty(pairName, initialCandle.getCloseRate(), to));
        } else {
            bufferedModels.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

            final Map<LocalDateTime, CandleModel> modelsMap = bufferedModels.stream()
                    .collect(Collectors.toMap(CandleModel::getCandleOpenTime, Function.identity()));

            initialCandle = bufferedModels.get(0);
            if (from.isBefore(initialCandle.getCandleOpenTime())) {
                CandleModel previousCandle = getPreviousCandle(pairName, from, interval);

                initialCandle = nonNull(previousCandle) ? previousCandle : CandleModel.empty(pairName, BigDecimal.ZERO, null);
            }

            while (from.isBefore(to)) {
                CandleModel model = modelsMap.get(from);

                if (isNull(model)) {
                    bufferedModels.add(CandleModel.empty(pairName, initialCandle.getCloseRate(), from));
                } else {
                    initialCandle = model;
                }
                from = from.plusMinutes(minutes);
            }
            if (isNull(modelsMap.get(to))) {
                bufferedModels.add(CandleModel.empty(pairName, initialCandle.getCloseRate(), to));
            }
        }
        bufferedModels.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        return bufferedModels;
    }

    @Override
    public LocalDateTime getLastCandleTimeBeforeDate(String pairName, LocalDateTime candleDateTime, BackDealInterval interval) {
        if (isNull(candleDateTime)) {
            return null;
        }

        candleDateTime = getNearestBackTimeForBackdealInterval(candleDateTime, interval);

        final LocalDate boundaryTime = getBoundaryTime(interval);

        if (candleDateTime.isAfter(boundaryTime.atTime(0, 0))) {
            final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

            return redisProcessingService.getLastCandleTimeBeforeDate(candleDateTime, hashKey, interval);
        } else {
            final String id = ElasticsearchGeneratorUtil.generateId(pairName);

            return elasticsearchProcessingService.getLastCandleTimeBeforeDate(candleDateTime, id);
        }
    }

    @Override
    public void handleReceivedTrades(String pairName, List<TradeDataDto> dto) {
        dto.stream()
                .collect(Collectors.groupingBy(p -> TimeUtil.getNearestTimeBeforeForMinInterval(p.getTradeDate())))
                .values()
                .forEach(trades -> groupTradesAndSave(pairName, trades));
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

    private void groupTradesAndSave(String pairName, List<TradeDataDto> dto) {
        xSync.execute(pairName, () -> {
            CandleModel newModel = CandleDataConverter.reduceToCandle(dto);
            if (isNull(newModel)) {
                return;
            }

            supportedIntervals.forEach(interval -> {
                final LocalDateTime candleDateTime = TimeUtil.getNearestBackTimeForBackdealInterval(newModel.getCandleOpenTime(), interval);
                newModel.setCandleOpenTime(candleDateTime);

                final CandleModel previousCandle = getPreviousCandle(pairName, candleDateTime, interval);
                newModel.setOpenRate(previousCandle.getCloseRate());

                final String key = RedisGeneratorUtil.generateKey(candleDateTime.toLocalDate());
                final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

                List<CandleModel> cachedModels = redisProcessingService.get(key, hashKey, interval);

                if (!CollectionUtils.isEmpty(cachedModels)) {
                    cachedModels = new ArrayList<>(cachedModels);

                    CandleModel cachedModel = cachedModels.stream()
                            .filter(model -> model.getCandleOpenTime().isEqual(candleDateTime))
                            .peek(model -> CandleDataConverter.merge(model, newModel))
                            .findFirst()
                            .orElse(null);

                    if (Objects.isNull(cachedModel)) {
                        cachedModels.add(newModel);
                    }
                } else {
                    cachedModels = Collections.singletonList(newModel);
                }

                redisProcessingService.insertOrUpdate(cachedModels, key, hashKey, interval);
            });
        });
    }

    private CandleModel getPreviousCandle(String pairName, LocalDateTime candleDateTime, BackDealInterval interval) {
        final LocalDateTime defaultPreviousCandleTime = candleDateTime.minusMinutes(TimeUtil.convertToMinutes(interval));

        final LocalDate boundaryTime = getBoundaryTime(interval);

        CandleModel previousModel;
        if (candleDateTime.isAfter(boundaryTime.atTime(0, 0))) {
            final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

            LocalDateTime lastCandleTimeBeforeDate = redisProcessingService.getLastCandleTimeBeforeDate(candleDateTime, hashKey, interval);
            if (Objects.isNull(lastCandleTimeBeforeDate)) {
                return CandleModel.empty(pairName, BigDecimal.ZERO, defaultPreviousCandleTime);
            }

            final String key = RedisGeneratorUtil.generateKey(lastCandleTimeBeforeDate.toLocalDate());

            List<CandleModel> models = redisProcessingService.get(key, hashKey, interval);
            if (CollectionUtils.isEmpty(models)) {
                return CandleModel.empty(pairName, BigDecimal.ZERO, defaultPreviousCandleTime);
            }
            previousModel = models.stream()
                    .filter(model -> model.getCandleOpenTime().isEqual(lastCandleTimeBeforeDate))
                    .findFirst()
                    .orElse(CandleModel.empty(pairName, BigDecimal.ZERO, defaultPreviousCandleTime));
        } else {
            final String id = ElasticsearchGeneratorUtil.generateId(pairName);

            LocalDateTime lastCandleTimeBeforeDate = elasticsearchProcessingService.getLastCandleTimeBeforeDate(candleDateTime, id);
            if (Objects.isNull(lastCandleTimeBeforeDate)) {
                return CandleModel.empty(pairName, BigDecimal.ZERO, defaultPreviousCandleTime);
            }

            final String index = ElasticsearchGeneratorUtil.generateIndex(lastCandleTimeBeforeDate.toLocalDate());

            List<CandleModel> models = elasticsearchProcessingService.get(index, id);
            if (CollectionUtils.isEmpty(models)) {
                return CandleModel.empty(pairName, BigDecimal.ZERO, defaultPreviousCandleTime);
            }
            previousModel = models.stream()
                    .filter(model -> model.getCandleOpenTime().isEqual(lastCandleTimeBeforeDate))
                    .findFirst()
                    .orElse(CandleModel.empty(pairName, BigDecimal.ZERO, defaultPreviousCandleTime));
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

    private LocalDate getBoundaryTime(BackDealInterval interval) {
        LocalDateTime currentDateTime = LocalDateTime.now();

        return currentDateTime.minusMinutes(candlesToStoreInCache * TimeUtil.convertToMinutes(interval)).toLocalDate();
    }
}