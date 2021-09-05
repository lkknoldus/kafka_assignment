package com.knoldus.Serialisation;
import java.io.UnsupportedEncodingException;

import com.knoldus.Model.DataModel;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import java.util.Map;
public class PDeserializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {
        //Nothing to configure
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        DataModel dataModel = null;
        try {
            dataModel = objectMapper.readValue(data, DataModel.class);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataModel;
    }
}
