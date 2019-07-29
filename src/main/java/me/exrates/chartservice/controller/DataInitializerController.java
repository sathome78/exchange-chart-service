package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.services.DataInitializerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@Log4j2
@RequestMapping("/data-initializer")
@RestController
public class DataInitializerController {

    private final DataInitializerService initializerService;

    @Autowired
    public DataInitializerController(DataInitializerService initializerService) {
        this.initializerService = initializerService;
    }

    @PostMapping("/generate")
    public ResponseEntity generateDataByCriteria(@RequestParam(value = "from", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
                                                 @RequestParam(value = "to", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
                                                 @RequestParam("pairs") List<String> pairs) {
        try {
            log.debug("Criteria - from: {}, to: {}, pairs: {}", fromDate, toDate, pairs.toString());

            pairs.forEach(pair -> initializerService.generate(fromDate, toDate, pair));
            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            log.error("Some error occurred during generate chart data", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}