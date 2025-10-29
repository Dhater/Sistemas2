#!/bin/bash
# kafka-init.sh

echo "⏳ Esperando a que Kafka inicie..."
sleep 10

echo "✅ Creando tópicos..."

kafka-topics --create --topic preguntas \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 || true

kafka-topics --create --topic respuestas_exitosas \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 || true

kafka-topics --create --topic respuestas_fallidas \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 || true

echo "🏁 Tópicos verificados:"
kafka-topics --list --bootstrap-server kafka:9092
