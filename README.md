# E-commerce Event-Driven QA Framework (Java + Apache Kafka)

[![CI/CD Kafka QA Pipeline](https://github.com/Luizin16/E-commerce-Event-Driven-QA-Framework/actions/workflows/ci.yml/badge.svg)](https://github.com/Luizin16/E-commerce-Event-Driven-QA-Framework/actions)

Framework avançado de automação de testes focado na validação de arquiteturas orientadas a eventos (EDA) utilizando **Apache Kafka**, **Java 17**, **JUnit 5**, **RestAssured** e **Testcontainers**.

## 🎯 Diferenciais de Engenharia de Qualidade (SDET)

- **Orquestração Efêmera de Infraestrutura (Testcontainers):** Elimina a dependência de subida manual do ambiente (`docker compose`). Os containers de Kafka são provisionados e destruídos programmaticamente pelo ciclo de vida do JUnit.
- **Testes de Contrato (JSON Schema):** Validação estrutural de tipos, campos obrigatórios e enums para garantir compliance do evento publicado.
- **Validação Assíncrona Reativa (Awaitility):** Polling dinâmico sem esperas estáticas (`Thread.sleep`), eliminando flakiness nos testes.
- **Tratamento de Falhas e Resiliência (DLQ):** Testes focados em comportamentos de exceção e validação de roteamento para tópicos de *Dead Letter Queue*.

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Java 17
- **Test Runner:** JUnit 5
- **Provisionamento Dinâmico:** Testcontainers Java
- **Cliente Kafka:** Apache Kafka Clients
- **Validação Assíncrona:** Awaitility
- **Validação de Schema:** RestAssured JSON Schema Validator
- **Pipeline de CI/CD:** GitHub Actions

## 🚀 Como Executar Localmente

Graças ao **Testcontainers**, você não precisa subir nenhum arquivo do Docker Compose na mão. Basta manter o Docker Desktop aberto e rodar:

```bash
# Clonar o repositório
git clone [https://github.com/Luizin16/E-commerce-Event-Driven-QA-Framework.git](https://github.com/Luizin16/E-commerce-Event-Driven-QA-Framework.git)

# Executar a suíte completa de testes via Maven
mvn clean test