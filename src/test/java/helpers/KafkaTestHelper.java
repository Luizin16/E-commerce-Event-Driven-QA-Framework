package helpers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaTestHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestHelper.class);
    private static String bootstrapServers = "localhost:9092";

    public static void setBootstrapServers(String servers) {
        bootstrapServers = servers;
    }

    public static void enviarMensagem(String topico, String chave, String payload) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topico, chave, payload);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Mensagem enviada com sucesso para o topico {} [Partition: {}, Offset: {}]",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Erro ao enviar mensagem para o Kafka", exception);
                }
            }).get();
        } catch (Exception e) {
            log.error("Falha na execucao do Producer do Kafka", e);
            throw new RuntimeException("Falha ao enviar mensagem para o Kafka", e);
        }
    }

    public static List<ConsumerRecord<String, String>> consumirMensagens(String topico, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, String>> mensagens = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topico));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                mensagens.add(record);
            }
        } catch (Exception e) {
            log.error("Erro ao consumir mensagens do topico {}", topico, e);
        }

        return mensagens;
    }
}