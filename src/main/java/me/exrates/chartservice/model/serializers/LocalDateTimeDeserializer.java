package me.exrates.chartservice.model.serializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


public class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public LocalDateTime deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        String raw = jsonParser.getValueAsString();
        if (StringUtils.isEmpty(raw)) {
            return null;
        }
        String str = raw.replaceAll("\"", "");
        if (str.endsWith("Z")) {
            return ZonedDateTime.parse(str).toLocalDateTime();
        } else {
            try {
                try {
                    return LocalDateTime.parse(str);
                } catch (DateTimeParseException ex) {
                    return LocalDateTime.parse(str, FORMATTER);
                }
                //Additional catch block for InOut microservice
            } catch (DateTimeParseException ex){
                String[] dateTime = str.split(" ");
                return LocalDateTime.of(LocalDate.parse(dateTime[0]), LocalTime.parse(dateTime[1]));
            }
        }
    }
}
