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

from kafka import KafkaConsumer
from stats_info import StatsInfo
from flow_info import FlowInfo
from byte_codec import ByteCodec

class Exporter(multiprocessing.Process):
    messageType = 1
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
    
    def message_header_to_bytes(self):
        flowNumBytes = ByteCodec.short_to_bytes(self.flowNum)
        messageTypeBytes = ByteCodec.short_to_bytes(self.messageType)
        currTimeBytes = ByteCodec.int_to_bytes(int(time.time()))

    def run(self):
        si = StatsInfo()
        fi = FlowInfo(si)
        fi.flow_info_to_bytes()
        consumer = KafkaConsumer(bootstrap_servers='10.107.16.149:9092',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['voltha.kpis'])

        while not self.stop_event.is_set():
            for message in consumer:
                value = message.value
                if 'prefixes' not in value:
                    break

                content = value["prefixes"]
                dev_name = '0001b51ff90f79b7'
                (olt_nni, olt_pon) = self.build_olt_caption(dev_name)

                if olt_nni in content:
                    (tx_pkt_num, rx_pkt_num) = self.parse_metrics(content[olt_nni])
                    self.print_metrics('[OLT_NNI]', tx_pkt_num, rx_pkt_num)

                if olt_pon in content:
                    (tx_pkt_num, rx_pkt_num) = self.parse_metrics(content[olt_pon])
                    self.print_metrics('[OLT_PON]', tx_pkt_num, rx_pkt_num)

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

