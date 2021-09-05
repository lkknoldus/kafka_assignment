package com.knoldus.Serialisation;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;
import org.codehaus.jackson.map.ObjectMapper;
import java.util.Map;
public class PSerializer implements Serializer{
    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing to configure
    }
@Override
    public byte[] serialize(String topic , Object object) {
        byte[] data = null;
    ObjectMapper objectMapper = new ObjectMapper();
            try {
            data  = objectMapper.writeValueAsString(object).getBytes();

    } catch (Exception e) {
        throw new SerializationException("Error when serializing Supplier to byte[]");
    }
            return data;
}
@Override
    public void close(){


}
}
