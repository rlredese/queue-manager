#!/usr/bin/env python
from time import sleep
import requests
import json
from config import settings
import pika, sys, os
import subprocess

def create_work_instructiod(path: str) -> json:
    """"""
    with open("list_work_instruction.json","+r") as file:
        for item in file["list_path_work"]:
            if path in item:
                return item

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='179.127.167.148'))
    channel = connection.channel()

    channel.queue_declare(queue='process_queue',durable=True)

    def callback(ch, method, properties, body):
        try:
            
            method_frame = channel.basic_get(queue = 'process_queue')        
            message = json.loads(body)
            path_file = message.get("path")
            INST_WORK = create_work_instructiod(path_file)
            cmd = f"py {path_file} {INST_WORK}".format(path_file,INST_WORK)
            os.system(cmd)
            result = subprocess.check_output(cmd, shell=True, text=True)
            if result:
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            print(f" [x] Received {body}")
            
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