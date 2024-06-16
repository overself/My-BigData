package com.beam.project.core.convert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.joda.time.Instant;

import java.io.IOException;

public class JodaInstantDeserializer  extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return Instant.ofEpochMilli(p.getValueAsLong());
    }
}
