package me.exrates.chartservice.wrappers;

import lombok.extern.log4j.Log4j2;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;


@Log4j2
public class NamedParameterJdbcTemplateWrapper extends NamedParameterJdbcTemplate {

    public NamedParameterJdbcTemplateWrapper(DataSource dataSource) {
        super(dataSource);
    }

    private String logSql(String sql, Map<String, ?> paramMap) {
        for (Map.Entry<String, ?> entry : paramMap.entrySet()) {
            sql = sql.replace(":" + entry.getKey(), String.valueOf(entry.getValue()));
        }
        log.debug(sql);
        return sql;
    }

    @Override
    public <T> T execute(String sql, Map<String, ?> paramMap, PreparedStatementCallback<T> action) throws DataAccessException {
        String s = logSql(sql, paramMap);
        return super.execute(sql, paramMap, action);
    }

    @Override
    public <T> T query(String sql, Map<String, ?> paramMap, ResultSetExtractor<T> rse) throws DataAccessException {
        logSql(sql, paramMap);
        return super.query(sql, paramMap, rse);
    }

    @Override
    public void query(String sql, Map<String, ?> paramMap, RowCallbackHandler rch) throws DataAccessException {
        logSql(sql, paramMap);
        super.query(sql, paramMap, rch);
    }

    @Override
    public <T> List<T> query(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) throws DataAccessException {
        logSql(sql, paramMap);
        return super.query(sql, paramMap, rowMapper);
    }

    @Override
    public <T> T queryForObject(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) throws DataAccessException {
        logSql(sql, paramMap);
        return super.queryForObject(sql, paramMap, rowMapper);
    }

    @Override
    public <T> T queryForObject(String sql, Map<String, ?> paramMap, Class<T> requiredType) throws DataAccessException {
        logSql(sql, paramMap);
        return super.queryForObject(sql, paramMap, requiredType);
    }

    @Override
    public Map<String, Object> queryForMap(String sql, Map<String, ?> paramMap) throws DataAccessException {
        logSql(sql, paramMap);
        return super.queryForMap(sql, paramMap);
    }

    @Override
    public <T> List<T> queryForList(String sql, Map<String, ?> paramMap, Class<T> elementType) throws DataAccessException {
        logSql(sql, paramMap);
        return super.queryForList(sql, paramMap, elementType);
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql, Map<String, ?> paramMap) throws DataAccessException {
        logSql(sql, paramMap);
        return super.queryForList(sql, paramMap);
    }

    @Override
    public SqlRowSet queryForRowSet(String sql, Map<String, ?> paramMap) throws DataAccessException {
        logSql(sql, paramMap);
        return super.queryForRowSet(sql, paramMap);
    }

    @Override
    public int update(String sql, Map<String, ?> paramMap) throws DataAccessException {
        logSql(sql, paramMap);
        return super.update(sql, paramMap);
    }
}