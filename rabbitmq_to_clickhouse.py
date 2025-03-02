import clickhouse_connect
import pika
import json
from datetime import datetime

# Configuración de RabbitMQ
RABBITMQ_HOST = 'rabbitmq.railway.amqp://3DG5TI8cSct9mz84:1tbhh6v3kidlHODHaoAS1LArDBx.IuqS@junnet:46601'
RABBITMQ_QUEUE = 'clickhouse_queue'

# Configuración de ClickHouse
CLICKHOUSE_HOST = 'clickhouse-production-0732.up.railway.app'
CLICKHOUSE_PORT = 443
CLICKHOUSE_USER = 'clickhouse'
CLICKHOUSE_PASSWORD = 'zW00wtHXvt8wKcdCApITp0s9fMHCsfuH'
CLICKHOUSE_DATABASE = 'railway'
CLICKHOUSE_TABLE = 'bitacora'

# Conectar a ClickHouse
try:
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        secure=True 
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
        message = json.loads(body.decode('utf-8'))
        bitacora_data = message.get('bitacora')
        personas_data = message.get('personas', [])

        # Convertir la cadena de fecha y hora a un objeto datetime
        fechayHora_str = bitacora_data['fechayHora']
        fechayHora_dt = datetime.strptime(fechayHora_str, '%Y-%m-%dT%H:%M:%SZ')

        # Insertar en la tabla de bitácora
        clickhouse_client.insert(
            CLICKHOUSE_TABLE,
            [(
                bitacora_data['idTransaccion'],
                bitacora_data['idSolicitud'],
                bitacora_data['codTramite'],
                bitacora_data['nroTramite'],
                bitacora_data['usuarioConsumidor'],
                bitacora_data['entidadConsumidora'],
                bitacora_data['sistemaCosumidor'],
                bitacora_data['sistemaPublicador'],
                bitacora_data['servicio'],
                bitacora_data['entidadPublicadora'],
                fechayHora_dt, 
                bitacora_data['estado'],
                bitacora_data['codHTTP']
            )]
        )

        # Insertar en la tabla de personas
        if personas_data:
            clickhouse_client.insert(
                'personas',
                [(
                    bitacora_data['idTransaccion'],
                    persona['tipoId'],
                    persona['valorId'],
                    persona['complemento']
                ) for persona in personas_data]
            )

        print("Datos insertados en ClickHouse")
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")

# Conectar a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Crear la cola en RabbitMQ (si no existe)
channel.queue_declare(queue=RABBITMQ_QUEUE)

# Configurar el consumidor
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

print("Esperando mensajes. Presiona CTRL+C para salir.")
channel.start_consuming()