package me.exrates.chartservice.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import me.exrates.chartservice.wrappers.NamedParameterJdbcTemplateWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfiguration {

    @Value("${datasource.driver-class-name}")
    private String driverClassName;
    @Value("${datasource.url}")
    private String jdbcUrl;
    @Value("${datasource.username}")
    private String user;
    @Value("${datasource.password}")
    private String password;

    @Bean(name = "slaveHikariDataSource")
    public DataSource slaveHikariDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(driverClassName);
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        hikariConfig.setReadOnly(true);

        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setConnectionInitSql("SELECT 1");
        hikariConfig.setConnectionTimeout(5 * 1000);
        hikariConfig.setValidationTimeout(5 * 1000);
        hikariConfig.setIdleTimeout(5 * 60 * 1000);
        hikariConfig.setMaxLifetime(10 * 60 * 1000);
        hikariConfig.setMinimumIdle(5);
        hikariConfig.setMaximumPoolSize(25);
        hikariConfig.setInitializationFailTimeout(0);
        return new HikariDataSource(hikariConfig);
    }

    @DependsOn("slaveHikariDataSource")
    @Bean(name = "slaveTemplate")
    public NamedParameterJdbcOperations slaveNamedParameterJdbcTemplate(@Qualifier("slaveHikariDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplateWrapper(dataSource);
    }

    @Bean(name = "slaveTxManager")
    public PlatformTransactionManager slavePlatformTransactionManager() {
        return new DataSourceTransactionManager(slaveHikariDataSource());
    }
}