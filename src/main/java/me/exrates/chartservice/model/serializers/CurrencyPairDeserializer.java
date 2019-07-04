package me.exrates.chartservice.model.serializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

import static me.exrates.chartservice.utils.OpenApiUtils.transformCurrencyPairBack;

public class CurrencyPairDeserializer extends JsonDeserializer<String> {

    @Override
    public String deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        String raw = jsonParser.getValueAsString();
        return transformCurrencyPairBack(raw);
    }
}
