from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Configuration
import json

BROKER = "kafka:9092"
MAX_RETRIES = 3

# Configuración para incluir el jar del conector Kafka
config = Configuration()
config.set_string("pipeline.jars", "file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.19.jar")

env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

# --- Definir fuente Kafka (lee de respuestas_fallidas)
source = (
    KafkaSource.builder()
    .set_bootstrap_servers(BROKER)
    .set_topics("respuestas_fallidas")
    .set_group_id("flink_reprocessor_group")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# --- Definir sink Kafka (escribe a preguntas)
sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(BROKER)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("preguntas")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .build()
)

# --- Lógica de reprocesamiento
def reprocess_fails(value):
    data = json.loads(value)
    retries = data.get("retries", 0)
    if retries < MAX_RETRIES:
        data["retries"] = retries + 1
        print(f"🔁 Reintentando ID={data['id']} (intento {data['retries']})")
        return json.dumps(data)
    else:
        print(f"❌ ID={data['id']} descartado tras 3 intentos")
        return None

# --- Crear el flujo de datos
ds = env.from_source(source, watermark_strategy=None, type_info=Types.STRING())
(
    ds.map(reprocess_fails, output_type=Types.STRING())
    .filter(lambda v: v is not None)
    .sink_to(sink)
)

env.execute("Flink Reprocessor")
