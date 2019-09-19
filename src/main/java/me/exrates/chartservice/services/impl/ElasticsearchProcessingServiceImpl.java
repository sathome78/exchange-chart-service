package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.ModelList;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
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
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return false;
        }
    }

    @Override
    public List<CandleModel> get(String index, String id) {
        GetRequest request = new GetRequest(index, id);

        try {
            GetResponse response = client.get(request, RequestOptions.DEFAULT);

            String sourceAsString = response.getSourceAsString();
            if (isNull(sourceAsString)) {
                return null;
            }
            return getModels(sourceAsString);
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return null;
        }
    }

    @Override
    public LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, LocalDateTime boundaryTime, String id) {
        return getLastCandleTimeBeforeDate(candleDateTime, boundaryTime.toLocalDate(), candleDateTime.toLocalDate(), id);
    }

    private LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, LocalDate boundaryDate, LocalDate date, String id) {
        if (date.isBefore(boundaryDate)) {
            return null;
        }

        final String index = ElasticsearchGeneratorUtil.generateIndex(date);

        List<CandleModel> models = get(index, id);
        if (!CollectionUtils.isEmpty(models)) {
            LocalDateTime lastCandleTime = models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .filter(candleOpenTime -> candleOpenTime.isBefore(candleDateTime))
                    .max(Comparator.naturalOrder())
                    .orElse(null);

            if (Objects.nonNull(lastCandleTime)) {
                return lastCandleTime;
            }
        }
        return getLastCandleTimeBeforeDate(candleDateTime, boundaryDate, date.minusDays(1), id);
    }

    @Override
    public void bulkInsertOrUpdate(Map<String, List<CandleModel>> mapOfModels, String id) {
        BulkRequest bulkRequest = new BulkRequest();

        mapOfModels.forEach((index, models) -> {
            if (!this.existsIndex(index)) {
                this.createIndex(index);
            }

            String sourceString = getSourceString(models);
            if (nonNull(sourceString)) {
                IndexRequest request = new IndexRequest(index)
                        .id(id)
                        .source(sourceString, XContentType.JSON);
                bulkRequest.add(request);
            }
        });

        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
        }
    }

    @Override
    public void insert(List<CandleModel> models, String index, String id) {
        String sourceString = getSourceString(models);
        if (isNull(sourceString)) {
            return;
        }

        if (!this.existsIndex(index)) {
            this.createIndex(index);
        }

        IndexRequest request = new IndexRequest(index)
                .id(id)
                .source(sourceString, XContentType.JSON);

        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
        }
    }

    @Override
    public void update(List<CandleModel> models, String index, String id) {
        String sourceString = getSourceString(models);
        if (isNull(sourceString)) {
            return;
        }

        UpdateRequest request = new UpdateRequest(index, id)
                .doc(sourceString, XContentType.JSON);

        try {
            client.update(request, RequestOptions.DEFAULT);
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
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
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

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
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());
        }
    }

    @Override
    public String createIndex(String index) {
        CreateIndexRequest request = new CreateIndexRequest(index)
                .settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 0)
                        .build());

        try {
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

            return response.index();
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return null;
        }
    }

    @Override
    public boolean existsIndex(String index) {
        GetIndexRequest request = new GetIndexRequest(index);

        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException | ElasticsearchStatusException ex) {
            log.error("Problem with getting response from elasticsearch cluster: {}", ex.getMessage());

            return false;
        }
    }

    private String getSourceString(final List<CandleModel> models) {
        try {
            return mapper.writeValueAsString(new ModelList(models));
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing model object to string", ex);

            return null;
        }
    }

    private List<CandleModel> getModels(final String sourceString) {
        try {
            ModelList modelList = mapper.readValue(sourceString, ModelList.class);

            return modelList.getModels();
        } catch (IOException ex) {
            log.error("Problem with getting response from elasticsearch cluster", ex);

            return null;
        }
    }
}