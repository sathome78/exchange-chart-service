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
public class UpdateCacheJob {

    private final CacheDataInitializerService initializerService;

    @Autowired
    public UpdateCacheJob(CacheDataInitializerService initializerService) {
        this.initializerService = initializerService;
    }

    @Scheduled(cron = "${scheduled.update-cache}")
    public void updateCache() {
        try {
            initializerService.updateCache();
        } catch (Exception ex) {
            log.error("--> Job 'updateCache()' occurred error", ex);
        }
    }
}