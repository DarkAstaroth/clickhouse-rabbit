import pika
from clickhouse_driver import Client
from datetime import datetime

# Configuración de RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'clickhouse_queue'

# Configuración de ClickHouse
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'fhccfnxdy2'  # Deja vacío si no hay contraseña
CLICKHOUSE_DATABASE = 'database_db'
CLICKHOUSE_TABLE = 'mensajes'

# Conectar a ClickHouse
clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)

# Crear la tabla en ClickHouse (si no existe)
clickhouse_client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
    id UInt32,
    mensaje String,
    fecha DateTime
) ENGINE = MergeTree()
ORDER BY id;
""")

# Función para procesar mensajes de RabbitMQ
def callback(ch, method, properties, body):
    mensaje = body.decode('utf-8')
    print(f"Mensaje recibido: {mensaje}")

    # Insertar el mensaje en ClickHouse
    clickhouse_client.execute(
        f"INSERT INTO {CLICKHOUSE_TABLE} (id, mensaje, fecha) VALUES",
        [(1, mensaje, datetime.strptime('2023-10-01 00:00:00', '%Y-%m-%d %H:%M:%S'))]
    )
    print("Mensaje insertado en ClickHouse")

# Conectar a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Crear la cola en RabbitMQ (si no existe)
channel.queue_declare(queue=RABBITMQ_QUEUE)

# Configurar el consumidor
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

print("Esperando mensajes. Presiona CTRL+C para salir.")
channel.start_consuming()