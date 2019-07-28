package me.exrates.chartservice.repositories.impl;

import lombok.extern.log4j.Log4j2;
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
    public List<OrderDto> getFilteredOrders(LocalDate fromDate, LocalDate toDate, String pairName) {
        String currencyPairClause = " AND cp.name = :pairName ";

        String acceptedClause = StringUtils.EMPTY;
        if (Objects.nonNull(fromDate) && Objects.nonNull(toDate)) {
            acceptedClause = " AND (o.date_acception BETWEEN :dateFrom AND :dateTo) ";
        } else if (Objects.nonNull(fromDate)) {
            acceptedClause = " AND o.date_acception >= :dateFrom ";
        } else if (Objects.nonNull(toDate)) {
            acceptedClause = " AND o.date_acception <= :dateTo ";
        }

        String sql = "SELECT " +
                "o.id AS order_id, " +
                "cp.name AS currency_pair_name, " +
                "o.amount_base, " +
                "o.amount_convert, " +
                "o.exrate, " +
                "o.date_acception " +
                "FROM EXORDERS o " +
                "JOIN CURRENCY_PAIR cp ON cp.id = o.currency_pair_id " +
                "WHERE o.status_id = 3 "
                + currencyPairClause
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
                .amountBase(rs.getBigDecimal("amount_base"))
                .amountConvert(rs.getBigDecimal("amount_convert"))
                .exRate(rs.getBigDecimal("exrate"))
                .dateAcception(rs.getTimestamp("date_acception").toLocalDateTime())
                .build());
    }
}