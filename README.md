ClickHouse
```
docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 -p 9009:9009 clickhouse/clickhouse-server
```
- Cambiar contraseña  
```
docker exec -it clickhouse-server bash
nano /etc/clickhouse-server/users.xml 


# cambiar contraseña en <password><password>

```

- Ejecutar cliente de clickhouse
```
docker exec -it clickhouse-server clickhouse-client --host localhost --port 9000 --user default --password <pass>

```

Rabbit:
```
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
