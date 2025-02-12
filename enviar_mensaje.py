import pika

# Configuración de RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'clickhouse_queue'

# Conectar a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Enviar un mensaje
mensaje = "Hola, ClickHouse desde RabbitMQ!"
channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=mensaje)
print(f"Mensaje enviado: {mensaje}")

# Cerrar la conexión
connection.close()