package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
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

import static java.util.Objects.isNull;

@Log4j2
@Service
public class ElasticsearchProcessingServiceImpl implements ElasticsearchProcessingService {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    private static final String INDEX = "chart";

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
    public boolean exist(String pairName, LocalDateTime dateTime) {
        try {
            GetRequest request = new GetRequest(INDEX, pairName, dateTime.format(FORMATTER));

            return client.existsSource(request, RequestOptions.DEFAULT);
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster", ex);
            return false;
        }
    }

    @Override
    public CandleModel get(String pairName, LocalDateTime dateTime) {
        try {
            GetRequest request = new GetRequest(INDEX, pairName, dateTime.format(FORMATTER));

            GetResponse response = client.get(request, RequestOptions.DEFAULT);

            return mapper.readValue(response.getSourceAsString(), CandleModel.class);
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster", ex);
            return null;
        }
    }

    @Override
    public void insert(CandleModel model, String pairName) {
        xSync.execute(pairName, () -> {
            String sourceString = getSourceString(model);
            if (isNull(sourceString)) {
                return;
            }
            IndexRequest request = new IndexRequest(INDEX, pairName, model.getCandleOpenTime().format(FORMATTER))
                    .source(sourceString, XContentType.JSON);

            IndexResponse response;
            try {
                response = client.index(request, RequestOptions.DEFAULT);
            } catch (IOException ex) {
                log.error("Problem with getting response from elasticsearch cluster", ex);
                return;
            }

            if (response.getResult() != DocWriteResponse.Result.CREATED) {
                log.warn("Source have not created in elasticsearch cluster");
            }
        });
    }

    @Override
    public void update(CandleModel model, String pairName) {
        xSync.execute(pairName, () -> {
            String sourceString = getSourceString(model);
            if (isNull(sourceString)) {
                return;
            }
            UpdateRequest request = new UpdateRequest(INDEX, pairName, model.getCandleOpenTime().format(FORMATTER))
                    .doc(sourceString, XContentType.JSON);

            UpdateResponse response;
            try {
                response = client.update(request, RequestOptions.DEFAULT);
            } catch (IOException ex) {
                log.error("Problem with getting response from elasticsearch cluster", ex);
                return;
            }

            if (response.getResult() != DocWriteResponse.Result.UPDATED) {
                log.warn("Source have not updated in elasticsearch cluster");
            }
        });
    }

    private String getSourceString(final CandleModel model) {
        try {
            return mapper.writeValueAsString(model);
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing model object to string", ex);
            return null;
        }
    }

    @Override
    public long deleteAll() {
        try {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(INDEX);

            BulkByScrollResponse response = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);

            return response.getDeleted();
        } catch (IOException ex) {
            log.warn("Problem with getting response from elasticsearch cluster", ex);

            return 0L;
        }
    }

    @Override
    public List<CandleModel> getByQuery(LocalDateTime fromDate, LocalDateTime toDate, String pairName) {
        try {
            SearchRequest request = new SearchRequest()
                    .source(new SearchSourceBuilder()
                            .query(QueryBuilders.typeQuery(pairName))
                            .query(QueryBuilders.rangeQuery("candleOpenTime").from(fromDate).to(toDate)));

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            return getSearchResult(response);
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