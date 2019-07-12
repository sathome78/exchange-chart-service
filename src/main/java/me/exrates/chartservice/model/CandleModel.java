package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.client.ml.job.util.TimeUtil;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;
import static me.exrates.chartservice.utils.TimeUtil.getNearestTimeBeforeForMinInterval;


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
}
