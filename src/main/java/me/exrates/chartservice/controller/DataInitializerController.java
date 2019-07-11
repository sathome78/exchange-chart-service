package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.services.DataInitializerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_PRODUCES;

@Log4j2
@Profile(MODULE_MODE_PRODUCES)
@RequestMapping("/data-initializer")
@RestController
public class DataInitializerController {

    private final DataInitializerService initializerService;

    @Autowired
    public DataInitializerController(DataInitializerService initializerService) {
        this.initializerService = initializerService;
    }

    @PostMapping("/generate")
    public ResponseEntity generateDataByCriteria(@RequestParam(value = "from", required = false) LocalDate fromDate,
                                                 @RequestParam(value = "to", required = false) LocalDate toDate,
                                                 @RequestParam("pair_name") String pairName,
                                                 @RequestParam(value = "regenerate", required = false, defaultValue = "false") boolean regenerate) {
        try {
            log.debug("Criteria - from: {}, to: {}, pair name: {}, regenerate data: {}", fromDate, toDate, pairName, regenerate);

            initializerService.generate(fromDate, toDate, pairName, regenerate);
            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            log.error("Some error occurred during generate chart data", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}