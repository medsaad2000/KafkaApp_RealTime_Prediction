# Application to predict diabete in Real time via a Machine Learning model and using Apache KAFKA & FastAPI

* This application based on the **KAFKA API** (Apache KAFKA Streams), allowing real-time processing of data produced in real time launched by “Producers” then consumed by “Consumers”

* The Application predict the diabete in RealTime using a ML Model built using Scikit-Learn (Logistic Regression).

* The Kafka Consumer is used inside a Python Web API built using FastAPI to generate routes that could be used in a front end .

* Pima Indians Diabetes Database was the dataset that we used to train the model

## Technologies :

The implementation was done in ```python>=3.7``` using the web framework ```FastAPI```, and for interacting with Kafka the ```aiokafka``` library was chosen.

The ML Model was built using ```Scikit-Learn``` library

## Before Starting the application :
The first step will be to have Kafka broker and zookeeper running,
1. Start the Kafka broker :

The Docker Compose file above will run everything for you via Docker.

Start the Kafka broker : 
```
docker-compose up -d
```
The bootstrap server is expected to be running on localhost:9092

2. Create a topic :

Run this command to create a new topic 
```
docker exec broker \
kafka-topics --bootstrap-server broker:9092 \
             --create \
             --topic quickstart
```

## Run the application :
1. the following environment variable KAFKA_TOPIC should be defined with desired for the topic used to send messages.
```
$ export KAFKA_TOPIC=<my_topic>
```

2. Start the Web API which is the KAFKA Consumer by running:

```
$ python main.py
```

3. Run the KAFKA PRODUCER

```
python producer.py
```






