package omg.example;

import omg.example.listener.SaveOffsetsOnRebalance;
import omg.example.model.Account;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static omg.example.BasicPropFactory.genDefaultCusProp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountTransactionConsumer {
    private static final Logger log = LoggerFactory.getLogger(AccountTransactionConsumer.class.getName());
    public static void main(String[] args) {
        log.info("info");
        log.debug("debug");
        log.error("error");
        log.warn("warn");
        log.trace("trace");
        KafkaConsumer<String, Integer> consumer = getKafkaConsumer();

        setShutdownHook(consumer);

        Map<TopicPartition, OffsetAndMetadata> offsetsToFlush = new HashMap<>();

        //consumer.subscribe(Arrays.asList("account-point-transaction"));
        //consumer.subscribe(List.of("account-point-transaction"));
        //consumer.subscribe(Collections.singletonList("account-point-transaction"));
        consumer.subscribe(Collections.singletonList("account-point-transaction"), new SaveOffsetsOnRebalance(consumer, offsetsToFlush));

        int total_transactionCount = 0;
        Map<String, Account> accounts = getAccounts();
        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Integer> record : records) {
                    handleRecord(record, accounts);

                    total_transactionCount++;

                    //consumer.commitAsync();
                    handleOffset(record, offsetsToFlush);
                    if(total_transactionCount % 500 == 0)
                        flushOffsetscommitAsync(consumer, offsetsToFlush);
                }
                if(!offsetsToFlush.isEmpty()) {
                    log.info("commit remaining offset before the next poll");
                    flushOffsetscommitAsync(consumer, offsetsToFlush);
                }
            }
        }
        catch (WakeupException e){
            log.info("consumer woke up !!");
        }
        catch (Exception e){
            log.info("Unexpected error: " + e.getMessage());
        }
        finally {
            accounts.forEach((name, acc) -> {
                log.info(acc.toString());
            });
            log.info("total_transactionCount: " + total_transactionCount);

            //consumer.commitSync();
            consumer.commitSync(offsetsToFlush);
            consumer.close();
            log.info("consumer.close() and bye bye");
        }
    }

    private static KafkaConsumer<String, Integer> getKafkaConsumer(){
        String groupId = "account-point-cg";
        Map<String,Object> prop = genDefaultCusProp();
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(prop);
    }

    private static void handleRecord(ConsumerRecord<String, Integer> record, Map<String, Account> accounts){
        String name = record.key();
        Integer pointChanged = record.value();
        log.info("Key: " + name + ", Value: " + pointChanged + "\n" +
                "Topic: " + record.topic() + ", Partition: " + record.partition() + ", Offset:" + record.offset() + "\n");

        Account acc = accounts.get(name);
        if(acc == null) {
            acc = new Account(name);
            accounts.put(name, acc);
        }
        acc.changePoint(pointChanged);
    }
    private static void flushOffsetscommitAsync(KafkaConsumer<String, Integer> consumer, Map<TopicPartition, OffsetAndMetadata> offsetsToFlush){
        consumer.commitAsync(offsetsToFlush, null);
        offsetsToFlush.clear();
    }
    private static Map<String, Account> getAccounts(){
        Map<String, Account> accounts = new HashMap<>();
        String[] accountNames = new String[]{
                "Sophie Gonzalez",
                "Tyler Schultz",
                "Gail Steele",
                "Edward Santiago",
                "Erica White",
                "Fred Payne",
                "Gwen Thomas",
                "Tamara Shelton",
                "Sarah Little",
                "Flora Summers"
        };

        log.info("init result: ");
        for(String accName : accountNames) {
            Account acc = new Account(accName);
            accounts.put(accName, acc);
            log.info(acc.toString());
        }

        return accounts;
    }
    private static void handleOffset(ConsumerRecord<?,?> record, Map<TopicPartition, OffsetAndMetadata> offsetsToFlush){
        offsetsToFlush.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        );
    }
    private static int randomInt(int s, int e){
        return ThreadLocalRandom.current().nextInt(s,e);
    }
    private static void setShutdownHook(KafkaConsumer<?, ?> consumer){
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
