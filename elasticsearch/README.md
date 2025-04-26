# Despliegue de  ElasticSearch con Docker

Crear una red Docker (opcional, pero recomendado), se permite que los contenedores se comuniquen fácilmente entre sí (en caso de haber varios):
```bash
docker network create elastic
```

Levantar los servicios (dentro de la carpeta _/elasticsearch_), esto descargará las imágenes:
```bash
docker-compose up -d
```

Verificar que funciona.
 - Elasticsearch: http://localhost:9200
 - Kibana: http://localhost:5601
