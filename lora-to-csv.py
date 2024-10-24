#!/usr/bin/env python3

import argparse
import json
import sys
import csv
import ssl
import threading
import time
from typing import List, Dict
import paho.mqtt.client as mqtt

def parse_arguments():
    parser = argparse.ArgumentParser(description='MQTT to CSV Converter')
    parser.add_argument('callsign', type=str, help='RX callsign to subscribe to')
    parser.add_argument('--host', type=str, default='hydros.link9.net', help='MQTT broker host')
    parser.add_argument('--port', type=int, default=8183, help='MQTT broker port')
    parser.add_argument('--topic', type=str, default='lora_aprs', help='Base MQTT topic')
    parser.add_argument('--username', type=str, default=None, help='MQTT username if required')
    parser.add_argument('--password', type=str, default=None, help='MQTT password if required')
    return parser.parse_args()

class MQTTToCSV:
    def __init__(self, broker_host: str, broker_port: int, topic: str, callsign: str,
                 username: str = None, password: str = None, output_file: str = 'lora-syslog.csv'):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.base_topic = topic
        self.callsign = callsign
        self.username = username
        self.password = password
        self.output_file = output_file

        self.client = mqtt.Client(transport="websockets")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.headers_written = False
        self.lock = threading.Lock()

        # Open the CSV file in write mode to empty it at the start
        try:
            self.file_handle = open(self.output_file, 'w', newline='', encoding='utf-8')
            self.csv_file_writer = csv.writer(self.file_handle)
        except Exception as e:
            print(f"Failed to open output file '{self.output_file}': {e}", file=sys.stderr)
            sys.exit(1)

        # CSV writer for stdout
        self.csv_stdout_writer = csv.writer(sys.stdout)

        # If authentication is required
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        # Set TLS/SSL parameters if needed
        self.client.tls_set(cert_reqs=ssl.CERT_NONE)
        self.client.tls_insecure_set(True)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            subscribe_topic = f"{self.base_topic}/{self.callsign}/+/json_message"
            client.subscribe(subscribe_topic)
            print(f"Connected to MQTT broker at {self.broker_host}:{self.broker_port}", file=sys.stderr)
            print(f"Subscribed to topic: {subscribe_topic}", file=sys.stderr)
        else:
            print(f"Failed to connect, return code {rc}", file=sys.stderr)
            sys.exit(1)

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)

            # Extract tx_callsign from the topic
            topic_parts = msg.topic.split('/')
            if len(topic_parts) >= 4:
                # The topic structure is: lora_aprs/<rx_callsign>/<tx_callsign>/json_message
                tx_callsign = topic_parts[2]
            else:
                tx_callsign = ""
                print(f"Unexpected topic format: {msg.topic}", file=sys.stderr)

            # Overwrite tx_callsign in data with the one from the topic
            data['tx_callsign'] = tx_callsign

            # Add rx_callsign from the command line argument
            data['rx_callsign'] = self.callsign

            with self.lock:
                if not self.headers_written:
                    # Define the desired header order
                    headers = ['timestamp', 'rx_callsign', 'tx_callsign']
                    # Add remaining keys, excluding 'timestamp', 'rx_callsign', and 'tx_callsign'
                    self.remaining_keys = [k for k in data.keys() if k not in headers]
                    headers.extend(self.remaining_keys)

                    # Write headers to both stdout and the file
                    self.csv_stdout_writer.writerow(headers)
                    self.csv_file_writer.writerow(headers)
                    self.headers_written = True

                # Prepare the row in the defined header order
                row = [
                    data.get('timestamp', ''),
                    data.get('rx_callsign', ''),
                    data.get('tx_callsign', '')
                ]
                # Add the remaining fields
                for key in self.remaining_keys:
                    value = data.get(key, '')
                    # If the value is a list or dict, convert it to a JSON string
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    row.append(value)

                # Write the row to both stdout and the file
                self.csv_stdout_writer.writerow(row)
                self.csv_file_writer.writerow(row)
                sys.stdout.flush()
                self.file_handle.flush()
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Error processing message: {e}", file=sys.stderr)

    def start(self):
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        except Exception as e:
            print(f"Could not connect to MQTT broker: {e}", file=sys.stderr)
            sys.exit(1)

        # Start the network loop in a separate thread
        self.client.loop_start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Disconnecting from MQTT broker...", file=sys.stderr)
            self.client.disconnect()
            self.client.loop_stop()
            print("Disconnected.", file=sys.stderr)
            self.file_handle.close()

def main():
    args = parse_arguments()
    mqtt_to_csv = MQTTToCSV(
        broker_host=args.host,
        broker_port=args.port,
        topic=args.topic,
        callsign=args.callsign,
        username=args.username,
        password=args.password,
        output_file='lora-syslog.csv'  # Specify the output file name
    )
    mqtt_to_csv.start()

if __name__ == '__main__':
    main()

