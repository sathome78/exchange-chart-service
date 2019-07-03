package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Log4j2
@Service
public class ElasticsearchProcessingServiceImpl implements ElasticsearchProcessingService {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    private static final String INDEX = "_chartData";

    private final RestHighLevelClient client;
    private final ObjectMapper mapper;
    private final XSync<String> xSync;

    @Autowired
    public ElasticsearchProcessingServiceImpl(RestHighLevelClient client,
                                              @Qualifier("jsonMapper") ObjectMapper mapper,
                                              XSync<String> xSync) {
        this.client = client;
        this.mapper = mapper;
        this.xSync = xSync;
    }

    @Override
    public void send(CandleModel model, String pairName) {
        xSync.execute(pairName, () -> {
            String sourceString;
            try {
                sourceString = mapper.writeValueAsString(model);
            } catch (JsonProcessingException ex) {
                log.warn("Problem with write model object to string", ex);
                return;
            }

            final String id = model.getCandleOpenTime().format(FORMATTER);

            try {
                GetRequest getRequest = new GetRequest(INDEX, pairName, id);

                boolean exist = client.existsSource(getRequest, RequestOptions.DEFAULT);

                if (exist) {
                    UpdateRequest updateRequest = new UpdateRequest(INDEX, pairName, id)
                            .doc(sourceString, XContentType.JSON);

                    UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

                    if (updateResponse.getResult() != DocWriteResponse.Result.UPDATED) {
                        log.warn("Source have not updated in elasticsearch cluster");
                    }
                } else {
                    IndexRequest indexRequest = new IndexRequest(INDEX, pairName, id)
                            .source(sourceString, XContentType.JSON);

                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                    if (indexResponse.getResult() != DocWriteResponse.Result.CREATED) {
                        log.warn("Source have not created in elasticsearch cluster");
                    }
                }
            } catch (IOException ex) {
                log.warn("Problem with getting response from elasticsearch cluster", ex);
            }
        });
    }

    @Override
    public List<CandleModel> get(LocalDateTime fromDate, LocalDateTime toDate, String pairName) {
        try {
            SearchRequest searchRequest = new SearchRequest()
                    .indices(INDEX)
                    .source(new SearchSourceBuilder()
                            .query(QueryBuilders.typeQuery(pairName))
                            .query(QueryBuilders.rangeQuery("candleOpenTime").from(fromDate).to(toDate)));

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return getSearchResult(searchResponse);
        } catch (IOException ex) {
            log.warn("Problem with getting response from elasticsearch cluster", ex);

            return Collections.emptyList();
        }
    }

    private List<CandleModel> getSearchResult(SearchResponse response) {

        SearchHit[] searchHit = response.getHits().getHits();

        List<CandleModel> chartData = new ArrayList<>();

        if (searchHit.length > 0) {
            Arrays.stream(searchHit)
                    .forEach(hit -> {
                                CandleModel candleMadel;
                                try {
                                    candleMadel = mapper.readValue(hit.getSourceAsString(), CandleModel.class);
                                } catch (IOException ex) {
                                    log.warn("Problem with read model object from string", ex);
                                    return;
                                }
                                chartData.add(candleMadel);
                            }
                    );
        }
        return chartData;
    }
}