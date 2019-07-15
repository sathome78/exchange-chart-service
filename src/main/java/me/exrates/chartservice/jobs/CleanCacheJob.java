package me.exrates.chartservice.jobs;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.services.CacheDataInitializerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_CONSUMES;

@EnableScheduling
@Profile(MODULE_MODE_CONSUMES)
@Log4j2
@Component
public class CleanCacheJob {

    private final CacheDataInitializerService initializerService;

    @Autowired
    public CleanCacheJob(CacheDataInitializerService initializerService) {
        this.initializerService = initializerService;
    }

    @Scheduled(cron = "${scheduled.clean-cache}")
    public void cleanCache() {
        try {
            initializerService.cleanCache();
        } catch (Exception ex) {
            log.error("--> Job 'cleanCache()' occurred error", ex);
        }
    }
}