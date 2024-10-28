# Kafka Data Pipeline Flink
Data pipeline written by flink to transfer Kafka to Kafka, Doris and also merge the two data sources.  

## Overview

- Platform: JDK 11
- Build Tool: Apache Maven v3.9.6
- Data Processing Framework: Flink v1.18.1


## Run
Use IntelliJ IDEA  
![IntelliJ IDEA](IntelliJIDEA.png)


## Entry

### 1. KafkaToKafka

topic1 in localhost:9092 -> topic2 in localhost:9092  


### 2. KafkaToDoris

- Kafka Data Structure
```
{
    "location": "Area A",
    "timestamp": "2024-03-25T08:00:00",
    "data": [
        {
            "sensorId": "sensor001",
            "sensorType": "Temperature",
            "value": 25.5,
            "unit": "Celsius"
        },
        {
            "sensorId": "sensor002",
            "sensorType": "Humidity",
            "value": 60.2,
            "unit": "%"
        }
    ]
}
```

- Doris table

| id        | type          | location    | timestamp           | value | unit    |  
|-----------|---------------|-------------|---------------------|-------|---------|  
| sensor001 | Temperature   | Area A      | 2024-03-25T08:00:00 | 25.5  | Celsius |  
| sensor002 | Humidity      | Area A      | 2024-03-25T08:00:00 | 60.2  | %       |  



### 3. TwoKafkaToDoris

- Kafka Data Structure V1
```
{
    "location": "Area A",
    "timestamp": "2024-03-25T08:00:00",
    "data": [
        {
            "sensorId": "sensor001",
            "sensorType": "Temperature",
            "value": 25.5,
            "unit": "Celsius"
        },
        {
            "sensorId": "sensor002",
            "sensorType": "Humidity",
            "value": 60.2,
            "unit": "%"
        }
    ]
}
```

- Kafka Data Structure V2
```
{
    "equipments": [
        {
            "id": "equipment001",
            "name": "機器1",
            "location": "Area A"
        }
    ],
    "sensors": [
        {
            "id": "sensor001",
            "equipments": ["equipment001", "equipment002"]
        },
        {
            "id": "sensor002",
            "equipments": ["equipment001", "equipment003"]
        }
    ]
}
```

- Doris table

| equipment_id  | sensor_id | sensor_type   | sensor_timestamp      | sensor_value | sensor_unit  |  
|---------------|-----------|---------------|-----------------------|--------------|--------------|  
| equipment001  | sensor001 | Temperature   | 2024-05-02T08:00:00   | 25.5         | Celsius      |  
| equipment001  | sensor002 | Humidity      | 2024-05-02T08:00:00   | 60.2         | %            |  
