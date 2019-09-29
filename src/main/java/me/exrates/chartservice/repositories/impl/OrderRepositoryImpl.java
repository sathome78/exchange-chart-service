package me.exrates.chartservice.repositories.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.repositories.OrderRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
    public List<CurrencyPairDto> getAllCurrencyPairNames() {
        final String sql = "SELECT " +
                "cp.name, " +
                "cp.hidden, " +
                "cp.market, " +
                "cp.scale, " +
                "cp.top_market, " +
                "cp.top_market_volume " +
                "FROM CURRENCY_PAIR cp";

        return slaveJdbcTemplate.query(sql, (rs, i) -> CurrencyPairDto.builder()
                .name(rs.getString("name"))
                .hidden(rs.getBoolean("hidden"))
                .matket(rs.getString("market"))
                .scale(rs.getBigDecimal("scale"))
                .topMarket(rs.getBoolean("top_market"))
                .topMarketVolume(rs.getBigDecimal("top_market_volume"))
                .build());
    }

    @Override
    public List<OrderDto> getFilteredOrders(LocalDate fromDate, LocalDate toDate, String pairName) {
        String acceptedClause = StringUtils.EMPTY;
        if (Objects.nonNull(fromDate) && Objects.nonNull(toDate)) {
            acceptedClause = " AND (o.date_acception BETWEEN :dateFrom AND :dateTo) ";
        } else if (Objects.nonNull(fromDate)) {
            acceptedClause = " AND o.date_acception >= :dateFrom ";
        } else if (Objects.nonNull(toDate)) {
            acceptedClause = " AND o.date_acception <= :dateTo ";
        }

        final String sql = "SELECT " +
                "o.id AS order_id, " +
                "cp.name AS currency_pair_name, " +
                "o.exrate, " +
                "o.amount_base, " +
                "o.amount_convert, " +
                "o.date_acception, " +
                "o.date_creation, " +
                "o.operation_type_id, " +
                "o.status_id " +
                "FROM EXORDERS o " +
                "JOIN CURRENCY_PAIR cp ON cp.id = o.currency_pair_id " +
                "WHERE cp.name = :pairName "
                + acceptedClause;

        Map<String, Object> params = new HashMap<>();
        params.put("pairName", pairName);

        if (Objects.nonNull(fromDate) && Objects.nonNull(toDate)) {
            params.put("dateFrom", fromDate.atTime(LocalTime.MIN));
            params.put("dateTo", toDate.minusDays(1).atTime(LocalTime.MAX));
        } else if (Objects.nonNull(fromDate)) {
            params.put("dateFrom", fromDate.atTime(LocalTime.MIN));
        } else if (Objects.nonNull(toDate)) {
            params.put("dateTo", toDate.minusDays(1).atTime(LocalTime.MAX));
        }

        return slaveJdbcTemplate.query(sql, params, (rs, rowNum) -> OrderDto.builder()
                .id(rs.getInt("order_id"))
                .currencyPairName(rs.getString("currency_pair_name"))
                .exRate(rs.getBigDecimal("exrate"))
                .amountBase(rs.getBigDecimal("amount_base"))
                .amountConvert(rs.getBigDecimal("amount_convert"))
                .dateAcception(Objects.nonNull(rs.getTimestamp("date_acception"))
                        ? rs.getTimestamp("date_acception").toLocalDateTime()
                        : null)
                .dateCreation(Objects.nonNull(rs.getTimestamp("date_creation"))
                        ? rs.getTimestamp("date_creation").toLocalDateTime()
                        : null)
                .operationTypeId(rs.getInt("operation_type_id"))
                .statusId(rs.getInt("status_id"))
                .build());
    }
}