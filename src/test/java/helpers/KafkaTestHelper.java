package helpers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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

/**
 * Utilitário para automação de testes QA com Apache Kafka.
 * Responsável por publicar eventos e consumir mensagens em tópicos.
 */
public class KafkaTestHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestHelper.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    /**
     * Publica uma mensagem (evento) em um tópico do Kafka de forma síncrona.
     *
     * @param topico Nome do tópico alvo (ex: pedidos-criados)
     * @param chave  Chave da mensagem / Partition key (ex: ID do pedido)
     * @param payload Conteúdo JSON do evento
     */
    public static void enviarMensagem(String topico, String chave, String payload) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Exige confirmação de entrega do broker

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topico, chave, payload);

            // O .get() força o envio síncrono para garantir que o evento foi publicado antes do teste prosseguir
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Mensagem publicada com sucesso! Tópico: {} | Partição: {} | Offset: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Erro ao publicar mensagem no Kafka", exception);
                }
            }).get();
        } catch (Exception e) {
            log.error("Falha na execução do Producer do Kafka", e);
            throw new RuntimeException("Falha ao enviar mensagem para o Kafka", e);
        }
    }

    /**
     * Consome e retorna as mensagens recentes de um tópico do Kafka.
     *
     * @param topico Nome do tópico a ser lido
     * @param groupId ID do grupo consumidor
     * @return Lista de ConsumerRecord com as mensagens capturadas
     */
    public static List<ConsumerRecord<String, String>> consumirMensagens(String topico, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Lê desde o início do tópico

        List<ConsumerRecord<String, String>> mensagens = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topico));

            // Poll com timeout de 3 segundos para ler mensagens disponíveis
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Mensagem capturada | Chave: {} | Valor: {}", record.key(), record.value());
                mensagens.add(record);
            }
        } catch (Exception e) {
            log.error("Erro ao consumir mensagens do tópico {}", topico, e);
        }

        return mensagens;
    }
}