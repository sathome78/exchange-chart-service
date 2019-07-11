package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Log4j2
@Service(value = "cacheDataInitService")
public class CacheDataInitServiceImpl implements CacheDataInitService {

    @PostConstruct
    private void init() {
        /*check if init needed by paritular profile or app arguments*/
        if (false/**/) {
            /*rabbit not consume messages because listener depends on this bean*/
            /*Write code for get defined amount of candles, aggregate them and save them to redis*/
            /*end init, and start rabbit consuming*/
        }
    }

}
