package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ActualCandleDataStorageService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.stereotype.Service;

import static me.exrates.chartservice.utils.TimeUtils.getNearestTimeBefore;


@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final ActualCandleDataStorageService storageService;
    private final XSync<String> xSync;

    public TradeDataServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                ActualCandleDataStorageService storageService,
                                XSync<String> xSync) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.storageService = storageService;
        this.xSync = xSync;
    }

    @Override
    public void handleReceivedTrade(TradeDataDto dto) {
        xSync.execute(dto.getPairName(), () -> {
            if (elasticsearchProcessingService.exist(dto.getPairName(), getNearestTimeBefore(dto.getTradeDate()))) {
                CandleModel model = elasticsearchProcessingService.get(dto.getPairName(), getNearestTimeBefore(dto.getTradeDate()));
                updateValuesFromNewTrade(dto, model);
                elasticsearchProcessingService.update(model, dto.getPairName());
            } else {
                CandleModel candleModel = CandleModel.newCandleFromTrade(dto);
                elasticsearchProcessingService.insert(candleModel, dto.getPairName());
            }
        });
    }

    private void updateValuesFromNewTrade(TradeDataDto tradeDataDto, CandleModel model) {
        model.setCloseRate(tradeDataDto.getExrate());
        model.setVolume(model.getVolume().add(tradeDataDto.getAmountBase()));
        model.setHighRate(model.getHighRate().max(tradeDataDto.getExrate()));
        model.setLowRate(model.getLowRate().min(tradeDataDto.getExrate()));
    }
}