# 🧠 Plataforma de Análisis de Preguntas y Respuestas

**Universidad Diego Portales — Sistemas Distribuidos, Entrega 2**  
**Integrantes:** Leandro Norambuena, Gonzalo Gaete  
**Profesor:** Nicolás Hidalgo  
**Fecha:** 30 de octubre de 2025

---

## 📘 Descripción del Proyecto

Este proyecto implementa un **pipeline distribuido** para la evaluación de preguntas y respuestas en línea, integrando **FastAPI**, **Kafka** y un **modelo de lenguaje (LLM)** para la generación automática de respuestas.

El objetivo principal es **comparar respuestas humanas con las generadas por el modelo**, evaluando su calidad mediante métricas cuantitativas como:

- Similitud  
- Completitud  
- Calidad general

La plataforma está diseñada para ser **modular, escalable y tolerante a fallos**, utilizando **contenedores Docker** para desplegar los distintos componentes y **APIs RESTful** para la comunicación entre ellos.

---

## 🏗 Arquitectura del Sistema

### Componentes Principales

1. **Generador de Tráfico**  
   - Produce datos con distribución configurable.  
   - Envía mensajes al tópico `preguntas` en Kafka.  
   - Recibe respuestas desde los tópicos `respuestas_exitosas` y `respuestas_fallidas`.

2. **Pipeline de Mensajería (Kafka + Zookeeper)**  
   - Gestiona la **ingestión y transporte de datos** entre componentes.  
   - Garantiza **entrega confiable y persistente** de mensajes.

3. **Procesamiento Distribuido (Flink)**  
   - Reprocesa respuestas fallidas, incrementando el contador de reintentos.  
   - Reenvía mensajes al tópico `preguntas` para nueva evaluación.

4. **API (FastAPI)**  
   - Consume mensajes del tópico `preguntas`.  
   - Genera respuestas y las publica en `respuestas_exitosas` o `respuestas_fallidas`.

5. **Almacenamiento (PostgreSQL / Redis)**  
   - Guarda preguntas, respuestas y métricas.  
   - Redis se considera opcional para cachear resultados.

---

## 🔄 Flujo de Datos

1. El **generador de tráfico** produce y envía datos a Kafka.  
2. **Kafka** distribuye los mensajes a los consumidores correspondientes.  
3. **FastAPI** procesa las preguntas y publica los resultados.  
4. **Flink** reprocesa mensajes fallidos y los reenvía.  
5. Los resultados se almacenan en **PostgreSQL** (y opcionalmente en Redis).

---

## ⚙ Tecnologías Utilizadas

| Tecnología     | Rol en el Proyecto |
|----------------|------------------|
| Python         | Lógica general y scripts de backend |
| FastAPI        | API REST asíncrona |
| Kafka / Zookeeper | Mensajería distribuida |
| Flink          | Procesamiento paralelo de datos |
| PostgreSQL     | Persistencia de resultados |
| Redis          | Cacheo de respuestas (opcional) |
| Docker         | Contenerización y despliegue modular |

---

## 📈 Escalabilidad y Paralelización

- **Kafka** permite múltiples productores y consumidores concurrentes.  
- **Flink** maneja reprocesamientos en paralelo sin bloquear la API.  
- El sistema es **horizontalmente escalable y tolerante a fallos**.

---

## 🧩 Diseño Modular

Cada módulo puede ejecutarse de forma independiente, lo que permite:

- Desarrollo y pruebas aisladas.  
- Escalado horizontal según la carga.  
- Reutilización en otros proyectos.  

Gracias a **Docker**, cada componente se ejecuta en su contenedor, facilitando el despliegue y las pruebas controladas.

---

## 📚 Referencias

- [Kafka Documentation](https://kafka.apache.org/documentation/)  
- [Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)  
- [FastAPI Documentation](https://fastapi.tiangolo.com/)  
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)  
- [Redis Documentation](https://redis.io/documentation)

---

## 🚀 Ejecución

```bash
# Levantar el entorno completo
docker-compose up --build

# Detener los contenedores
docker-compose down
