from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json

BROKER = "kafka:9092"

env = StreamExecutionEnvironment.get_execution_environment()

consumer_fail = FlinkKafkaConsumer(
    topics='respuestas_fallidas',
    deserialization_schema=SimpleStringSchema(),
    properties={'bootstrap.servers': BROKER, 'group.id': 'flink_group'}
)

producer_preguntas = FlinkKafkaProducer(
    topic='preguntas',
    serialization_schema=SimpleStringSchema(),
    producer_config={'bootstrap.servers': BROKER}
)

def reprocess_fails(value):
    data = json.loads(value)
    retries = data.get("retries", 0)
    if retries < 3:
        data["retries"] = retries + 1
        print(f"🔁 Reintentando ID={data['id']} (intento {data['retries']})")
        return json.dumps(data)
    else:
        print(f"❌ ID={data['id']} descartado tras 3 intentos")
        return None

stream = env.add_source(consumer_fail)
stream.map(reprocess_fails).filter(lambda v: v is not None).add_sink(producer_preguntas)

env.execute("Reprocesador de Respuestas Fallidas")
