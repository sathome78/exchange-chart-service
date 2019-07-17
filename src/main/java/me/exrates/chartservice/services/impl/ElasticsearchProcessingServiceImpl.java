package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static me.exrates.chartservice.configuration.CommonConfiguration.JSON_MAPPER;

@Log4j2
@Service
public class ElasticsearchProcessingServiceImpl implements ElasticsearchProcessingService {

    private static final String ALL = "_all";

    private final RestHighLevelClient client;
    private final ObjectMapper mapper;

    @Autowired
    public ElasticsearchProcessingServiceImpl(RestHighLevelClient client,
                                              @Qualifier(JSON_MAPPER) ObjectMapper mapper) {
        this.client = client;
        this.mapper = mapper;
    }

    @Override
    public List<String> getAllIndices() {
        GetIndexRequest request = new GetIndexRequest(ALL);

        try {
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);

            return Arrays.asList(response.getIndices());
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public boolean exists(String index, String id) {
        GetRequest request = new GetRequest(index, id);

        try {
            return client.existsSource(request, RequestOptions.DEFAULT);
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
            return false;
        }
    }

    @Override
    public CandleModel get(String index, String id) {
        GetRequest request = new GetRequest(index, id);

        try {
            GetResponse response = client.get(request, RequestOptions.DEFAULT);

            return mapper.readValue(response.getSourceAsString(), CandleModel.class);
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
            return null;
        }
    }

    @Override
    public List<CandleModel> getAllByIndex(String index) {
        SearchRequest request = new SearchRequest(index);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            return getSearchResult(response);
        } catch (IOException ex) {
            log.warn("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return Collections.emptyList();
        }
    }

    @Override
    public List<CandleModel> getByRange(LocalDateTime fromDate, LocalDateTime toDate, String index) {
        SearchRequest request = new SearchRequest(index)
                .source(new SearchSourceBuilder()
                        .query(QueryBuilders.rangeQuery("time_in_millis")
                                .gte(Timestamp.valueOf(fromDate).getTime())
                                .lt(Timestamp.valueOf(toDate).getTime())));

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            return getSearchResult(response);
        } catch (IOException ex) {
            log.warn("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return Collections.emptyList();
        }
    }

    @Override
    public void insert(CandleModel model, String index) {
        String sourceString = getSourceString(model);
        if (isNull(sourceString)) {
            return;
        }
        final String id = ElasticsearchGeneratorUtil.generateId(model.getCandleOpenTime());

        IndexRequest request = new IndexRequest(index)
                .id(id)
                .source(sourceString, XContentType.JSON);

        IndexResponse response;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
            return;
        }

        if (response.getResult() != DocWriteResponse.Result.CREATED) {
            log.warn("Source have not created in elasticsearch cluster");
        }
    }

    @Override
    public void batchInsert(List<CandleModel> models, String pairName) {
        models.forEach(model -> this.insert(model, pairName));
    }

    @Override
    public void update(CandleModel model, String index) {
        String sourceString = getSourceString(model);
        if (isNull(sourceString)) {
            return;
        }
        final String id = ElasticsearchGeneratorUtil.generateId(model.getCandleOpenTime());

        UpdateRequest request = new UpdateRequest(index, id)
                .doc(sourceString, XContentType.JSON);

        UpdateResponse response;
        try {
            response = client.update(request, RequestOptions.DEFAULT);
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
            return;
        }

        if (response.getResult() != DocWriteResponse.Result.UPDATED) {
            log.warn("Source have not updated in elasticsearch cluster");
        }
    }

    @Override
    public long deleteAllData() {
        return this.deleteDataByIndex(ALL);
    }

    @Override
    public long deleteDataByIndex(String index) {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index)
                .setQuery(QueryBuilders.matchAllQuery());

        try {
            BulkByScrollResponse response = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);

            return response.getDeleted();
        } catch (IOException ex) {
            log.warn("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return 0L;
        }
    }

    @Override
    public void deleteAllIndices() {
        this.deleteIndex(ALL);
    }

    @Override
    public void deleteIndex(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);

        try {
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

            if (!response.isAcknowledged()) {
                log.warn("Problem with deleting index: {}", index);
            }
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
        }
    }

    private List<CandleModel> getSearchResult(SearchResponse response) {
        return Arrays.stream(response.getHits().getHits())
                .map(hit -> {
                    try {
                        return mapper.readValue(hit.getSourceAsString(), CandleModel.class);
                    } catch (IOException ex) {
                        log.warn("Problem with read model object from string", ex);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toList());
    }

    private String getSourceString(final CandleModel model) {
        try {
            return mapper.writeValueAsString(model);
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing model object to string", ex);
            return null;
        }
    }
}