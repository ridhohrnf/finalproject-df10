from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep

def load_avro_schema_from_file():
    key_schema = avro.load("online_payment_key.avsc")
    value_schema = avro.load("online_payment_value.avsc")

    return key_schema, value_schema

def send_record():
    key_schema, value_schema = load_avro_schema_from_file()
    
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    file = open('D:\DS\DF\airflow_vanila\datasets\online_payment.csv')
    csvreader = csv.reader(file)
    header = next(csvreader)
    step_value = int(row[0])
    for row in csvreader:
            is_even = step_value % 2 == 0
            key = {"step":  step_value}
            value = {
                "step": step_value,
                "type": str(row[1]),
                "amount": float(row[2]),
                "nameOrig": str(row[3]),
                "oldbalanceOrg": float(row[4]),
                "newbalanceOrig": float(row[5]),
                "nameDest": str(row[6]),
                "oldbalanceDest": float(row[7]),
                "newbalanceDest": float(row[8]),
                "isFraud": int(row[9]),
                "isFlaggedFraud": int(row[10])
            }

            try:
                if is_even:
                    producer.produce(topic='com.online.payment', key=key, value=value)
            except Exception as e:
                print(f"Exception while producing record value - {value}: {e}")
            else:
                print(f"Successfully producing record value - {value}")

            producer.flush()
            sleep(1)

if __name__ == "__main__":
    send_record()
