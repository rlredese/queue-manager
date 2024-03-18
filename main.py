#!/usr/bin/env python
from time import sleep
import requests
import json
from config import settings
import pika, sys, os

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='process_queue',durable=True)

    def callback(ch, method, properties, body):
        try:
            method_frame = channel.basic_get(queue = 'process_queue')        
            message = json.loads(body)
            path_file = message.get("path")
            cmd = f"python {path_file} main.py".format(path_file)
            os.system(cmd)
           
            print(f" [x] Received {body}")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as error:
            return error
        
    channel.basic_consume(queue='process_queue', on_message_callback=callback, auto_ack=False)

    print(' [*] Start working, waiting for messages!')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        #try:
            #sys.exit(0)
        #except SystemExit:
            #os._exit(0)