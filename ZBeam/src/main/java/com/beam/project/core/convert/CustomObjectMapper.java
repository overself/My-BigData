package com.beam.project.core.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.joda.time.Instant;

public class CustomObjectMapper extends ObjectMapper {

    public CustomObjectMapper(){
        super();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(Instant.class, new JodaInstantSerializer());
        simpleModule.addDeserializer(Instant.class, new JodaInstantDeserializer());
        this.registerModule(simpleModule);
    }
}
