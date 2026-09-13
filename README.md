# 🚀 E-commerce Event-Driven QA Framework

![CI/CD Pipeline](https://github.com/Luizin16/E-commerce-Event-Driven-QA-Framework/actions/workflows/ci.yml/badge.svg)
![Allure Report Status](https://img.shields.io/badge/Allure%20Report-Live-brightgreen)
![Java](https://img.shields.io/badge/Java-17-orange)
![Testcontainers](https://img.shields.io/badge/Testcontainers-1.20-blue)

Framework de automação de testes para arquiteturas orientadas a eventos (Event-Driven Architecture - EDA), utilizando **Apache Kafka**, **Testcontainers**, **JUnit 5**, **Rest-Assured** e integração contínua via **GitHub Actions** com relatórios dinâmicos no **Allure Report**.

---

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Java 17
- **Test Runner:** JUnit 5 / Awaitility
- **Infraestrutura Efêmera:** Testcontainers (Confluent Kafka 7.5.0)
- **Validação de Contrato:** Rest-Assured JSON Schema Validator
- **Report & Dashboard:** Allure Report
- **Orquestração CI/CD:** GitHub Actions (Deploy automático via GitHub Pages)

---

## 📋 Cenários de Testes Automatizados

- **CT01 - Validação de Contrato (JSON Schema):** Publicação de evento de pedido e validação estrita da estrutura do payload contra o esquema JSON.
- **CT02 - Resiliência e Trata de Exceções (Dead Letter Queue):** Redirecionamento e captura de payloads corrompidos para a DLQ (`pedidos-dlq`).
- **CT03 - Teste de Idempotência:** Envio de eventos duplicados para verificação da consistência de consumo e chaves de lote.

---

## 📊 Relatório de Testes (Allure Report)

O dashboard interativo do Allure Report é gerado e atualizado automaticamente a cada commit realizado no repositório.

🔗 **Acesse o relatório em tempo real:** [Dashboard Allure Report](https://luizin16.github.io/E-commerce-Event-Driven-QA-Framework/)

---

## 🚀 Como Executar em Ambiente Local

### Pré-requisitos
- JDK 17+
- Maven 3.8+
- Docker Engine / Docker Desktop ativo

### Comando de Execução
```bash
mvn test