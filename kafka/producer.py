import subprocess
import sys
# subprocess.check_call([sys.executable, "-m", "pip", "uninstall", 'numpy'])
# subprocess.check_call([sys.executable, "-m", "pip", "install", 'numpy'])
import json
import pandas as pd
import threading
from kafka import KafkaProducer
from tqdm import tqdm


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding="utf-8"))
    # print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding="utf-8")).add_callback(
        success
    ).add_errback(error)
    producer.flush()


def produce_from_file(producer, file):
    data = pd.read_csv(file).drop(columns="Unnamed: 0")
    for index in data.index:

        kafka_python_producer_sync(
            producer, json.dumps(data.loc[index].to_dict()), "review"
        )


def run_job():
    producer = KafkaProducer(
        bootstrap_servers="34.68.76.63:9092"
    )  # use your VM's external IP Here!
    print('Sending Data!')
    # Change the path to your laptop!
    # if you want to learn about threading in python, check the following article
    # https://realpython.com/intro-to-python-threading/
    # if you want to schedule a job https://www.geeksforgeeks.org/python-schedule-library/
    t1 = threading.Thread(target=produce_from_file, args=(producer, "C:\Users\20202016\Documents\Master\Data Engineering\assignments\Data_Engineering_2\data\Review1.csv"))
    t2 = threading.Thread(target=produce_from_file, args=(producer, "C:\Users\20202016\Documents\Master\Data Engineering\assignments\Data_Engineering_2\data\Review2.csv"))
    t3 = threading.Thread(target=produce_from_file, args=(producer, "C:\Users\20202016\Documents\Master\Data Engineering\assignments\Data_Engineering_2\data\Review3.csv"))
    t4 = threading.Thread(target=produce_from_file, args=(producer, "C:\Users\20202016\Documents\Master\Data Engineering\assignments\Data_Engineering_2\data\Review4.csv"))
    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()


if __name__ == "__main__":
    run_job()
