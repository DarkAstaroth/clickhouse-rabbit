import clickhouse_connect
import pika
import json
from datetime import datetime
from urllib.parse import urlparse

# Configuración de RabbitMQ
RABBITMQ_URL = "amqp://3DG5TI8cSct9mz84:1tbhh6v3kidlHODHaoAS1LArDBx.IuqS@junction.proxy.rlwy.net:46601"
# amqp://3DG5TI8cSct9mz84:1tbhh6v3kidlHODHaoAS1LArDBx.IuqS@rabbitmq.railway.internal:5672
RABBITMQ_QUEUE = "clickhouse_queue"

# Extraer host, puerto y credenciales de la URL de RabbitMQ
parsed_url = urlparse(RABBITMQ_URL)
RABBITMQ_HOST = "junction.proxy.rlwy.net"
RABBITMQ_PORT = 46601
RABBITMQ_USER = "3DG5TI8cSct9mz84"
RABBITMQ_PASSWORD = "1tbhh6v3kidlHODHaoAS1LArDBx.IuqS"

# Configuración de ClickHouse
CLICKHOUSE_HOST = "clickhouse-4bj6-production.up.railway.app"
CLICKHOUSE_PORT = 443
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "IhaRmopenWm5lCav8huJkdmPF9bgApVN"
CLICKHOUSE_DATABASE = "railway"
CLICKHOUSE_TABLE = "bitacora"

# Conectar a ClickHouse
try:
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        secure=True,
    )
    print("Conexión exitosa a ClickHouse!")
except Exception as e:
    print(f"Error al conectar a ClickHouse: {e}")
    exit(1)

# Crear la tabla de bitácora en ClickHouse (si no existe)
clickhouse_client.command(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
    idTransaccion String,
    idSolicitud String,
    codTramite String,
    nroTramite String,
    usuarioConsumidor String,
    entidadConsumidora String,
    sistemaCosumidor String,
    sistemaPublicador String,
    servicio String,
    entidadPublicadora String,
    fechayHora DateTime,
    estado String,
    codHTTP UInt16
) ENGINE = MergeTree()
ORDER BY idTransaccion;
""")

# Crear la tabla de personas en ClickHouse (si no existe)
clickhouse_client.command(f"""
CREATE TABLE IF NOT EXISTS personas (
    idTransaccion String,
    tipoId String,
    valorId String,
    complemento String
) ENGINE = MergeTree()
ORDER BY idTransaccion;
""")


def callback(ch, method, properties, body):
    try:
        # Deserializar el mensaje
        message = json.loads(body.decode("utf-8"))
        bitacora_data = message.get("bitacora")
        personas_data = message.get("personas", [])

        print("Bitácora recibida:", json.dumps(bitacora_data, indent=4))

        # Convertir la cadena de fecha y hora a un objeto datetime
        fechayHora_str = bitacora_data["fechayHora"]
        fechayHora_dt = datetime.strptime(fechayHora_str, "%Y-%m-%dT%H:%M:%SZ")

        # Insertar en la tabla de bitácora
        clickhouse_client.insert(
            CLICKHOUSE_TABLE,
            [
                (
                    bitacora_data["idTransaccion"],
                    bitacora_data["idSolicitud"],
                    bitacora_data["codTramite"],
                    bitacora_data["nroTramite"],
                    bitacora_data["usuarioConsumidor"],
                    bitacora_data["entidadConsumidora"],
                    bitacora_data["sistemaConsumidor"],
                    bitacora_data["sistemaPublicador"],
                    bitacora_data["servicio"],
                    bitacora_data["entidadPublicadora"],
                    fechayHora_dt,
                    bitacora_data["estado"],
                    bitacora_data["codHTTP"],
                )
            ],
        )

        # Insertar en la tabla de personas
        if personas_data:
            clickhouse_client.insert(
                "personas",
                [
                    (
                        bitacora_data["idTransaccion"],
                        persona["tipoId"],
                        persona["valorId"],
                        persona["complemento"],
                    )
                    for persona in personas_data
                ],
            )

        print("Datos insertados en ClickHouse")
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")


# Conectar a RabbitMQ
try:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection_params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    print("Conexión exitosa a RabbitMQ!")
except Exception as e:
    print(f"Error al conectar a RabbitMQ: {e}")
    exit(1)

# Crear la cola en RabbitMQ (si no existe)
channel.queue_declare(queue=RABBITMQ_QUEUE)

# Configurar el consumidor
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

print("Esperando mensajes. Presiona CTRL+C para salir.")
channel.start_consuming()
