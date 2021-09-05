package com.knoldus.Producer_Consumer;
import java.io.FileWriter;
import java.time.Duration;
import java.util.*;

import com.knoldus.Model.DataModel;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.codehaus.jackson.map.ObjectMapper;

public class Consumer {
    public static void main(String[] args){

        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }
    public static void consumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.knoldus.Serialisation.PDeserializer");
        props.put("group.id", "test-group");
        KafkaConsumer<String, DataModel> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("Employee"));
        try {
            FileWriter file = new FileWriter("objects.txt");
            while (true) {
                FileWriter fileWriter = new FileWriter("objects.txt", true);
                ConsumerRecords<String, DataModel> records = kafkaConsumer.poll(Duration.ofMillis(100));
                ObjectMapper objectmapper = new ObjectMapper();
                for (ConsumerRecord<String, DataModel> record : records) {
                    System.out.println("Employee id= " + String.valueOf(record.value().getId()) + " Employee Name = " + record.value().getName() + " Employee Age = " + record.value().getAge() + " Employee Course = " + record.value().getCourse());
                    fileWriter.append(objectmapper.writeValueAsString(record.value()) + "\n");
                   
                }
                fileWriter.close();
 }
        }
       catch (Exception e){
            System.out.println(e);
        }
        finally {
            kafkaConsumer.close();
        }
        }
    }
    class ConsumerListener implements Runnable{
    @Override
        public void run(){
        Consumer.consumer();
    }
    }
