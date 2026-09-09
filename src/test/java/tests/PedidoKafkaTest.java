package tests;

import helpers.KafkaTestHelper;
import io.restassured.module.jsv.JsonSchemaValidator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class PedidoKafkaTest {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    private static final String TOPICO_PEDIDOS = "pedidos-criados";
    private static final String TOPICO_DLQ = "pedidos-dlq";

    @BeforeAll
    static void setUp() {
        KafkaTestHelper.setBootstrapServers(kafka.getBootstrapServers());
    }

    @Test
    @DisplayName("CT01 - Deve publicar evento e validar Contrato (JSON Schema)")
    void deveValidarContratoJsonSchemaDoEvento() {
        String pedidoId = "PED-" + UUID.randomUUID().toString().substring(0, 8);
        String payloadJson = String.format(
                "{\"pedidoId\": \"%s\", \"valor\": 180.00, \"status\": \"CRIADO\"}",
                pedidoId
        );

        KafkaTestHelper.enviarMensagem(TOPICO_PEDIDOS, pedidoId, payloadJson);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> mensagens =
                    KafkaTestHelper.consumirMensagens(TOPICO_PEDIDOS, "grupo-qa-schema");

            ConsumerRecord<String, String> evento = mensagens.stream()
                    .filter(m -> pedidoId.equals(m.key()))
                    .findFirst()
                    .orElse(null);

            assertNotNull(evento, "Evento não encontrado para o pedido: " + pedidoId);

            MatcherAssert.assertThat(
                    evento.value(),
                    JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/pedido-schema.json")
            );
        });
    }

    @Test
    @DisplayName("CT02 - Deve rotear mensagem corrompida para a Dead Letter Queue (DLQ)")
    void deveRoteasMensagemInvalidaParaDLQ() {
        String pedidoId = "PED-CORROMPIDO-" + UUID.randomUUID().toString().substring(0, 5);
        String payloadCorrompido = String.format("{\"pedidoId\": \"%s\", \"status\": \"DESCONHECIDO\"}", pedidoId);

        KafkaTestHelper.enviarMensagem(TOPICO_DLQ, pedidoId, payloadCorrompido);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> mensagensDlq =
                    KafkaTestHelper.consumirMensagens(TOPICO_DLQ, "grupo-qa-dlq");

            ConsumerRecord<String, String> eventoDlq = mensagensDlq.stream()
                    .filter(m -> pedidoId.equals(m.key()))
                    .findFirst()
                    .orElse(null);

            assertNotNull(eventoDlq, "A mensagem com erro deveria ter sido redirecionada para a DLQ!");
            assertTrue(eventoDlq.value().contains("DESCONHECIDO"));
        });
    }
}