package me.exrates.chartservice.repositories.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.CurrencyRateDto;
import me.exrates.chartservice.model.DailyDataDto;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.repositories.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Log4j2
@Repository
public class OrderRepositoryImpl implements OrderRepository {

    private final NamedParameterJdbcOperations slaveJdbcTemplate;

    @Autowired
    public OrderRepositoryImpl(@Qualifier("slaveTemplate") NamedParameterJdbcOperations slaveJdbcTemplate) {
        this.slaveJdbcTemplate = slaveJdbcTemplate;
    }

    @Override
    public List<CurrencyPairDto> getAllCurrencyPairs() {
        final String sql = "SELECT " +
                "cp.id, " +
                "cp.name, " +
                "cp.hidden, " +
                "cp.market, " +
                "cp.scale, " +
                "cp.top_market, " +
                "cp.top_market_volume " +
                "FROM CURRENCY_PAIR cp";

        return slaveJdbcTemplate.query(sql, currencyPairDtoRowMapper());
    }

    @Override
    public List<CurrencyPairDto> getCurrencyPairByName(String pairName) {
        final String sql = "SELECT " +
                "cp.id, " +
                "cp.name, " +
                "cp.hidden, " +
                "cp.market, " +
                "cp.scale, " +
                "cp.top_market, " +
                "cp.top_market_volume " +
                "FROM CURRENCY_PAIR cp " +
                "WHERE cp.name = :name";

        Map<String, Object> params = new HashMap<>();
        params.put("name", pairName);

        return slaveJdbcTemplate.query(sql, params, currencyPairDtoRowMapper());
    }

    private RowMapper<CurrencyPairDto> currencyPairDtoRowMapper() {
        return (rs, row) -> CurrencyPairDto.builder()
                .id(rs.getInt("id"))
                .name(rs.getString("name"))
                .hidden(rs.getBoolean("hidden"))
                .matket(rs.getString("market"))
                .scale(rs.getBigDecimal("scale"))
                .topMarket(rs.getBoolean("top_market"))
                .topMarketVolume(rs.getBigDecimal("top_market_volume"))
                .build();
    }

    @Override
    public List<CurrencyRateDto> getAllCurrencyRates() {
        final String sql = "SELECT " +
                "ccr.currency_name, " +
                "ccr.usd_rate, " +
                "ccr.btc_rate " +
                "FROM CURRENT_CURRENCY_RATES ccr";

        return slaveJdbcTemplate.query(sql, currencyRateDtoRowMapper());
    }

    @Override
    public List<CurrencyRateDto> getCurrencyRateByName(String currencyName) {
        final String sql = "SELECT " +
                "ccr.currency_name, " +
                "ccr.usd_rate, " +
                "ccr.btc_rate " +
                "FROM CURRENT_CURRENCY_RATES ccr " +
                "WHERE ccr.currency_name = :name";

        Map<String, Object> params = new HashMap<>();
        params.put("name", currencyName);

        return slaveJdbcTemplate.query(sql, params, currencyRateDtoRowMapper());
    }

    private RowMapper<CurrencyRateDto> currencyRateDtoRowMapper() {
        return (rs, row) -> CurrencyRateDto.builder()
                .currencyName(rs.getString("currency_name"))
                .usdRate(rs.getBigDecimal("usd_rate"))
                .btcRate(rs.getBigDecimal("btc_rate"))
                .build();
    }

    @Override
    public List<OrderDto> getClosedOrders(LocalDate from, LocalDate to, String pairName) {
        final String sql = "SELECT " +
                "cp.name AS currency_pair_name, " +
                "o.exrate, " +
                "o.amount_base, " +
                "o.amount_convert, " +
                "o.date_acception " +
                "FROM EXORDERS o " +
                "JOIN CURRENCY_PAIR cp ON cp.id = o.currency_pair_id " +
                "WHERE cp.name = :pairName AND o.status_id = 3 AND (o.date_acception BETWEEN :dateFrom AND :dateTo)";

        Map<String, Object> params = new HashMap<>();
        params.put("pairName", pairName);
        params.put("dateFrom", from.atTime(LocalTime.MIN));
        params.put("dateTo", to.minusDays(1).atTime(LocalTime.MAX));

        return slaveJdbcTemplate.query(sql, params, (rs, row) -> OrderDto.builder()
                .currencyPairName(rs.getString("currency_pair_name"))
                .exRate(rs.getBigDecimal("exrate"))
                .amountBase(rs.getBigDecimal("amount_base"))
                .amountConvert(rs.getBigDecimal("amount_convert"))
                .dateAcception(Objects.nonNull(rs.getTimestamp("date_acception"))
                        ? rs.getTimestamp("date_acception").toLocalDateTime()
                        : null)
                .build());
    }

    @Override
    public List<DailyDataDto> getDailyData(String pairName) {
        String whereClause = Objects.nonNull(pairName)
                ? "WHERE cp.name = :pair_name AND cp.hidden = 0"
                : "WHERE cp.hidden = 0";

        final String sql = "SELECT " +
                "cp.name AS currency_pair_name, " +
                "(SELECT MAX(hborder.exrate) AS highest_bid" +
                " FROM EXORDERS hborder" +
                " WHERE hborder.currency_pair_id = cp.id AND hborder.status_id = 2 AND hborder.operation_type_id = 4) AS highest_bid, " +
                "(SELECT MIN(laorder.exrate) AS lowest_ask" +
                " FROM EXORDERS laorder" +
                " WHERE laorder.currency_pair_id = cp.id AND laorder.status_id = 2 AND laorder.operation_type_id = 3) AS lowest_ask " +
                "FROM CURRENCY_PAIR cp " +
                whereClause +
                " GROUP BY cp.id";

        Map<String, Object> params = new HashMap<>();
        if (Objects.nonNull(pairName)) {
            params.put("pair_name", pairName);
        }
        return slaveJdbcTemplate.query(sql, params, (rs, row) -> DailyDataDto.builder()
                .currencyPairName(rs.getString("currency_pair_name"))
                .highestBid(rs.getBigDecimal("highest_bid"))
                .lowestAsk(rs.getBigDecimal("lowest_ask"))
                .build());
    }
}