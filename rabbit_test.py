import clickhouse_connect
import pika
import json
from datetime import datetime
from urllib.parse import urlparse

# Configuración de RabbitMQ
RABBITMQ_URL = "amqp://3DG5TI8cSct9mz84:1tbhh6v3kidlHODHaoAS1LArDBx.IuqS@junction.proxy.rlwy.net:46601"
RABBITMQ_QUEUE = "clickhouse_queue"

# Extraer credenciales de la URL de RabbitMQ
parsed_url = urlparse(RABBITMQ_URL)
RABBITMQ_HOST = parsed_url.hostname
RABBITMQ_PORT = parsed_url.port
RABBITMQ_USER = parsed_url.username
RABBITMQ_PASSWORD = parsed_url.password

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
    sistemaConsumidor String,
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
        bitacora_data = message.get("bitacora", {})
        personas_data = message.get("personas", [])

        print("Bitácora recibida:", json.dumps(bitacora_data, indent=4))

        # Convertir la cadena de fecha y hora a un objeto datetime
        fechayHora_str = bitacora_data.get("fechayHora", "")
        fechayHora_dt = (
            datetime.strptime(fechayHora_str, "%Y-%m-%dT%H:%M:%SZ")
            if fechayHora_str
            else datetime.utcnow()
        )

        # Reemplazar valores None con cadena vacía para evitar errores en ClickHouse
        bitacora_values = (
            str(bitacora_data.get("idTransaccion", "")) or "",
            str(bitacora_data.get("idSolicitud", "")) or "",
            str(bitacora_data.get("codTramite", "")) or "",
            str(bitacora_data.get("nroTramite", "")) or "",
            str(bitacora_data.get("usuarioConsumidor", "")) or "",
            str(bitacora_data.get("entidadConsumidora", "")) or "",
            str(bitacora_data.get("sistemaConsumidor", "")) or "",
            str(bitacora_data.get("sistemaPublicador", "")) or "",
            str(bitacora_data.get("servicio", "")) or "",
            str(bitacora_data.get("entidadPublicadora", "")) or "",
            fechayHora_dt,
            str(bitacora_data.get("estado", "")) or "",
            int(bitacora_data.get("codHTTP", 0)),
        )

        # Insertar en la tabla de bitácora
        clickhouse_client.insert(CLICKHOUSE_TABLE, [bitacora_values])

        # Insertar en la tabla de personas si existen datos
        if personas_data:
            personas_values = [
                (
                    str(bitacora_data.get("idTransaccion", "")) or "",
                    str(persona.get("tipoId", "")) or "",
                    str(persona.get("valorId", "")) or "",
                    str(persona.get("complemento", "")) or "",
                )
                for persona in personas_data
            ]
            clickhouse_client.insert("personas", personas_values)

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
