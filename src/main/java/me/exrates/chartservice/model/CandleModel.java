package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.utils.TimeUtils;
import org.elasticsearch.client.ml.job.util.TimeUtil;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static me.exrates.chartservice.utils.TimeUtils.getNearestBackTimeForBackdealInterval;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CandleModel {

    @JsonProperty("open")
    private BigDecimal openRate;
    @JsonProperty("closed")
    private BigDecimal closeRate;
    @JsonProperty("high")
    private BigDecimal highRate;
    @JsonProperty("low")
    private BigDecimal lowRate;
    private BigDecimal volume;
    @JsonProperty("last_trade_time")
    private LocalDateTime lastTradeTime;
    @JsonProperty("first_trade_time")
    private LocalDateTime firstTradeTime;
    @JsonProperty("time")
    private LocalDateTime candleOpenTime;
    @JsonProperty("time_in_millis")
    private long timeInMillis;

    public long getTimeInMillis() {
        return Timestamp.valueOf(candleOpenTime).getTime();
    }

    public static CandleModel newCandleFromTrade(TradeDataDto dto, BackDealInterval interval) {
        return CandleModel.builder()
                .candleOpenTime(getNearestBackTimeForBackdealInterval(dto.getTradeDate(), interval))
                .openRate(dto.getExrate())
                .closeRate(dto.getExrate())
                .highRate(dto.getExrate())
                .lowRate(dto.getExrate())
                .volume(dto.getAmountBase())
                .lastTradeTime(dto.getTradeDate())
                .firstTradeTime(dto.getTradeDate())
                .build();
    }


    public CandleModel(BigDecimal highRate, BigDecimal lowRate, BigDecimal volume) {
        this.highRate = highRate;
        this.lowRate = lowRate;
        this.volume = volume;
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
                .candleOpenTime(TimeUtils.getNearestTimeBeforeForMinInterval(tradeData.get(0).getTradeDate()))
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
                .candleOpenTime(cachedCandle.candleOpenTime)
                .openRate(leftStartFirst ? cachedCandle.getOpenRate() : newCandle.getOpenRate())
                .closeRate(leftEndLast ? cachedCandle.getCloseRate() : newCandle.getCloseRate())
                .volume(cachedCandle.getVolume().add(newCandle.getVolume()))
                .highRate(cachedCandle.getHighRate().max(newCandle.getHighRate()))
                .lowRate(cachedCandle.getLowRate().min(newCandle.getLowRate()))
                .build();

    }
}
