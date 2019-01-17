#!/usr/bin/env python3
'''
 Copyright 2018-present SK Telecom

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''

import threading, time
import multiprocessing
import json

import daemon
from daemon import pidfile
import logging
from logging import handlers

from kafka import KafkaConsumer, KafkaProducer
from stats_info import StatsInfo
from flow_info import FlowInfo
from byte_codec import ByteCodec
from util import get_size

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('daemon.log')
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

CONSUMER_BOOTSTRAP_SERVERS = '10.107.16.149:9092'
PRODUCER_BOOTSTRAP_SERVERS = '10.107.16.149:9092'

PRODUCER_API_VERSION = (0, 8, 2)

CONSUMER_KEY = ['voltha.kpis']
PRODUCER_TOPIC = 'tidc.data.flow'
PRODUCER_KEY = 'flowdata'

EXPORT_INTERVAL_SECOND = 5

LINGER_MS_CONFIG = 1
RETRY_BACKOFF_MS_CONFIG = 10000
RECONNECT_BACKOFF_MS_CONFIG = 10000

PON_DEVICE_NAME = '0001b51ff90f79b7'

class Metrics:
    metrics = {}

class Consumer(threading.Thread):
    def __init__(self):
        #multiprocessing.Process.__init__(self)
        #self.stop_event = multiprocessing.Event()
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def build_olt_caption(self, dev_caption):
        olt_caption = 'voltha.ponsim_olt'
        nni_caption = 'nni'
        pon_caption = 'pon'
        olt_nni_caption = "%s.%s.%s" % (olt_caption, dev_caption, nni_caption)
        olt_pon_caption = "%s.%s.%s" % (olt_caption, dev_caption, pon_caption)
        return (olt_nni_caption, olt_pon_caption)

    def parse_metrics(self, content):
        metrics = content["metrics"]
        tx_pkt_num = metrics["tx_65_127_pkts"]
        rx_pkt_num = metrics["rx_65_127_pkts"]
        return (tx_pkt_num, rx_pkt_num)

    def print_metrics(self, caption, tx_pkt_num, rx_pkt_num):
        result = caption + ' TX: ' + str(tx_pkt_num) + ' RX: ' + str(rx_pkt_num)
        logger.info(result)

    def print_size(self, caption, metrics_bytes):
        print (caption + str(get_size(metrics_bytes)))

    def gen_metrics_value(self, metrics_dict, metrics_key, curr_pkts, curr_bytes):
        existing = metrics_dict.get(metrics_key)
        metrics_value = {"prevAccPkts": existing.get("currAccPkts"), "prevAccBytes": existing.get("currAccBytes"), "currAccPkts": curr_pkts, "currAccBytes": curr_bytes}
        return metrics_value

    def gen_empty_metrics_value(self):
        empty_value = {"prevAccPkts": 0, "prevAccBytes": 0, "currAccPkts": 0, "currAccBytes": 0}
        return empty_value

    def run(self):

        # initialize kafka consumer
        consumer = KafkaConsumer(bootstrap_servers=CONSUMER_BOOTSTRAP_SERVERS,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(CONSUMER_KEY)

        while not self.stop_event.is_set():
            for message in consumer:
                value = message.value
                if 'prefixes' not in value:
                    break

                content = value["prefixes"]
                (olt_nni, olt_pon) = self.build_olt_caption(PON_DEVICE_NAME)

                if olt_nni in content:
                    (tx_pkt_num, rx_pkt_num) = self.parse_metrics(content[olt_nni])
                    self.print_metrics('[OLT_NNI]', tx_pkt_num, rx_pkt_num)
                    if olt_nni not in Metrics.metrics:
                        Metrics.metrics[olt_nni] = self.gen_empty_metrics_value()

                    Metrics.metrics[olt_nni] = self.gen_metrics_value(Metrics.metrics, olt_nni, tx_pkt_num, tx_pkt_num * 100)

                if olt_pon in content:
                    (tx_pkt_num, rx_pkt_num) = self.parse_metrics(content[olt_pon])
                    self.print_metrics('[OLT_PON]', tx_pkt_num, rx_pkt_num)
                    if olt_pon not in Metrics.metrics:
                        Metrics.metrics[olt_pon] = self.gen_empty_metrics_value()

                    Metrics.metrics[olt_pon] = self.gen_metrics_value(Metrics.metrics, olt_pon, tx_pkt_num, tx_pkt_num * 100)

                # print (Metrics.metrics)

                if self.stop_event.is_set():
                    break

        consumer.close()

class Producer(threading.Thread):
    messageType = 1
    flowNum = 1

    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
    
    def message_header_to_bytes(self):
        flowNumBytes = ByteCodec.short_to_bytes(self.flowNum)
        messageTypeBytes = ByteCodec.short_to_bytes(self.messageType)
        currTimeBytes = ByteCodec.int_to_bytes(int(time.time()))
        return flowNumBytes + messageTypeBytes + currTimeBytes

    def message_body_to_bytes(self, fi):
        return fi.flow_info_to_bytes()

    def metrics_to_bytes(self, metrics_key, metrics_value):
        si = StatsInfo()
        si.lstPktOffset = EXPORT_INTERVAL_SECOND * 1000
        si.currAccPkts = metrics_value["currAccPkts"]
        si.prevAccPkts = metrics_value["prevAccPkts"]
        si.currAccBytes = metrics_value["currAccBytes"]
        si.prevAccBytes = metrics_value["prevAccBytes"]

        fi = FlowInfo(si)
        fi.deviceId = 1
        fi.inputIntfId = 1
        fi.outputIntfId = 2
        fi.vlanId= 1
        fi.srcIp = '10.10.10.10'
        fi.dstIp = '20.20.20.20'
        fi.srcMac = 'a4:23:05:00:00:00'
        fi.dstMac = 'a4:23:05:00:00:11'

        messageHeader = self.message_header_to_bytes()
        messageBody = self.message_body_to_bytes(fi)

        return messageHeader + messageBody

    def stop(self):
        self.stop_event.set()

    def run(self):

        # initialize kafka producer
        producer = KafkaProducer(bootstrap_servers=PRODUCER_BOOTSTRAP_SERVERS,
                                 api_version=PRODUCER_API_VERSION,
                                 linger_ms=LINGER_MS_CONFIG,
                                 retry_backoff_ms=RETRY_BACKOFF_MS_CONFIG,
                                 reconnect_backoff_ms=RECONNECT_BACKOFF_MS_CONFIG,
                                 key_serializer=str.encode)

        while not self.stop_event.is_set():
            if not Metrics.metrics:
                logger.info("Nothing to publish...")
            else:
                for key, value in Metrics.metrics.items():
                    metrics = self.metrics_to_bytes(key, value)
                    producer.send(PRODUCER_TOPIC, key=PRODUCER_KEY, value=metrics)
                    logger.info("Metrics for " + key + " was published!")

            time.sleep(EXPORT_INTERVAL_SECOND)

        producer.close()

def main():
    tasks = [
        Consumer(),
        Producer()
    ]

    for t in tasks:
        t.start()

def start_daemon():
    with daemon.DaemonContext(
        working_directory='./',
        pidfile=pidfile.TimeoutPIDLockFile('daemon.pid'),
        ) as context:
        main()

if __name__ == "__main__":
    main()
