package com.knoldus.Producer_Consumer;

import java.util.*;

import com.knoldus.Model.DataModel;
import org.apache.kafka.clients.producer.*;

public class Producer {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.knoldus.Serialisation.PSerializer");
        KafkaProducer<String, DataModel> kafkaProducer = new KafkaProducer<>(props);
        try {
            DataModel emp1 = new DataModel(1, "Lokesh", 21, "Btech");
            DataModel emp2 = new DataModel(2, "chetan_bhagat", 24, "M.C.A.");
            kafkaProducer.send(new ProducerRecord<>("Employee", "EMP1", emp1));
            kafkaProducer.send(new ProducerRecord<>("Employee", "EMP2", emp2));
            System.out.println("EmployeeProducer Completed.");
            kafkaProducer.close();
        }
        catch (Exception e){
            System.out.println(e);
        }
        finally {
            kafkaProducer.close();
        }



    }
}