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

import threading, logging, time
import multiprocessing
import json

from kafka import KafkaConsumer, KafkaProducer
from stats_info import StatsInfo
from flow_info import FlowInfo
from byte_codec import ByteCodec
from util import get_size

CONSUMER_BOOTSTRAP_SERVERS = '10.107.16.149:9092'
PRODUCER_BOOTSTRAP_SERVERS = '10.107.16.149:9092'

CONSUMER_KEY = ['voltha.kpis']
PRODUCER_KEY = 'tidc.data.flow'

LINGER_MS_CONFIG = 1
RETRY_BACKOFF_MS_CONFIG = 10000
RECONNECT_BACKOFF_MS_CONFIG = 10000

PON_DEVICE_NAME = '0001b51ff90f79b7'

class Exporter(multiprocessing.Process):
    messageType = 1
    flowNum = 1    
 
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

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
        print (result)

    def print_size(self, caption, metrics_bytes):
        print (caption + str(get_size(metrics_bytes)))

    def message_header_to_bytes(self):
        flowNumBytes = ByteCodec.short_to_bytes(self.flowNum)
        messageTypeBytes = ByteCodec.short_to_bytes(self.messageType)
        currTimeBytes = ByteCodec.int_to_bytes(int(time.time()))
        return flowNumBytes + messageTypeBytes + currTimeBytes
    
    def message_body_to_bytes(self, fi):
        return fi.flow_info_to_bytes()

    def metrics_to_bytes(self, tx_pkt_num, rx_pkt_num):
        si = StatsInfo()
        si.lstPktOffset = 5000
        si.currAccPkts = tx_pkt_num
        si.prevAccPkts = tx_pkt_num
        si.currAccBytes = tx_pkt_num * 100
        si.prevAccBytes = tx_pkt_num * 100

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
    
    def run(self):
        global CONSUMER_BOOTSTRAP_SERVERS
        global PRODUCER_BOOTSTRAP_SERVERS

        global CONSUMER_KEY
        global PRODUCER_KEY

        global LINGER_MS_CONFIG
        global RETRY_BACKOFF_MS_CONFIG
        global RECONNECT_BACKOFF_MS_CONFIG
        global TOPIC
        global PON_DEVICE_NAME

        # initialize kafka consumer
        consumer = KafkaConsumer(bootstrap_servers=CONSUMER_BOOTSTRAP_SERVERS,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(CONSUMER_KEY)

        # initialize kafka producer
        producer = KafkaProducer(bootstrap_servers=PRODUCER_BOOTSTRAP_SERVERS,
                                 linger_ms=LINGER_MS_CONFIG,
                                 retry_backoff_ms=RETRY_BACKOFF_MS_CONFIG,
                                 reconnect_backoff_ms=RECONNECT_BACKOFF_MS_CONFIG)

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
                    metrics = self.metrics_to_bytes(tx_pkt_num, rx_pkt_num)

                if olt_pon in content:
                    (tx_pkt_num, rx_pkt_num) = self.parse_metrics(content[olt_pon])
                    self.print_metrics('[OLT_PON]', tx_pkt_num, rx_pkt_num)
                    metrics = self.metrics_to_bytes(tx_pkt_num, rx_pkt_num)

                if self.stop_event.is_set():
                    break

        consumer.close()

def main():
    tasks = [
        Exporter()
    ]

    for t in tasks:
        t.start()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARN
        )
    main()

