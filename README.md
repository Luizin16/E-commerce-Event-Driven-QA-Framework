# E-commerce Event-Driven QA Framework (Java + Apache Kafka)

Framework de automação de testes focado na validação de arquiteturas orientadas a eventos (EDA) utilizando **Apache Kafka**, **Java**, **JUnit 5**, **RestAssured** e **Awaitility**.

## 🎯 Tipos de Teste Implementados

- **Testes de Contrato (JSON Schema):** Validação de estrutura, presença de campos obrigatórios e conformidade de tipos de dados nos eventos do Kafka.
- **Validação de Mensageria Assíncrona:** Leitura e processamento de tópicos sem dependência de esperas fixas (`Thread.sleep`), utilizando polling reativo com `Awaitility`.
- **Tratamento de Falhas e Resiliência (DLQ):** Validação de redirecionamento de payloads inválidos/corrompidos para tópicos de *Dead Letter Queue*.

## 🛠️ Tecnologias

- **Linguagem:** Java 17
- **Test Runner:** JUnit 5
- **Cliente Kafka:** Apache Kafka Clients 3.6
- **Validação Assíncrona:** Awaitility 4.2
- **Validador de Contrato:** RestAssured JSON Schema Validator
- **Infraestrutura:** Docker Compose (Apache Kafka modo KRaft)

## 🚀 Como Executar

1. **Subir o Cluster Kafka local:**
   ```bash
   docker compose up -d
   
