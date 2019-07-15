package me.exrates.chartservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.exrates.chartservice.model.serializers.LocalDateTimeDeserializer;
import me.exrates.chartservice.model.serializers.LocalDateTimeSerializer;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;


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
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonProperty("last_trade_time")
    private LocalDateTime lastTradeTime;
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonProperty("first_trade_time")
    private LocalDateTime firstTradeTime;
    @JsonProperty("time")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime candleOpenTime;
    @JsonProperty("time_in_millis")
    private long timeInMillis;

    public long getTimeInMillis() {
        return Timestamp.valueOf(candleOpenTime).getTime();
    }

    public CandleModel(BigDecimal highRate, BigDecimal lowRate, BigDecimal volume) {
        this.highRate = highRate;
        this.lowRate = lowRate;
        this.volume = volume;
    }
}
