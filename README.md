# udata-search-service

A search service for udata.
The idea is to have search service separated from the udata MongoDB.
The indexation update is made using real-time messages with Kafka.

See the following architecture schema:
![Udata Search Service architecture schema](docs/udata-search-service-schema.png "Udata Search Service architecture schema")

## Getting started

Start the different services using docker-compose:
```
docker-compose up
```

This will start:
- an elasticsearch
- a kafka broker
- a zookeper
- a kafka consumer
- a search app


Initialize the elasticsearch indices
```
flask init-es
```

You can feed the elasticsearch by publishing messages to Kafka.
Using [udata](https://github.com/opendatateam/udata), when you modify objects,
indexation messages will be sent and will be consumed by the kafka consumer.

You can query the search service with http://localhost:5000/api/1/datasets/?q=toilettes%20à%20rennes
