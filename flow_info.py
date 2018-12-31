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

from stats_info import StatsInfo
from byte_codec import ByteCodec

class FlowInfo():
    flowType = b'\x00'
    deviceId = 0
    inputIntfId = 0
    outputIntfId = 0
    vlanId = 0
    vxlanId = 0
    srcIp = ""
    dstIp = ""
    srcIpLength = b'\x00'
    dstIpLength = b'\x00'
    srcPort = 0
    dstPort = 0
    protocol = b'\x00'
    srcMac = ""
    dstMac = ""
    statsInfo = StatsInfo()

    def __init__(self, statsInfoArg : StatsInfo):
        self.flowType = b'\x01'
        self.vlanId = 1
        self.vxlanId = 2
        self.protocol = b'\x01'
        statsInfo = statsInfoArg

    def flow_info_to_bytes(self):
        flowTypeBytes = self.flowType
        deviceIdBytes = ByteCodec.short_to_bytes(self.deviceId)
        inIntfIdBytes = ByteCodec.int_to_bytes(self.inputIntfId)
        outIntfIdBytes = ByteCodec.int_to_bytes(self.outputIntfId)
        vlanIdBytes = ByteCodec.short_to_bytes(self.vlanId)
        srcIpBytes = ByteCodec.ip_to_bytes(self.srcIp)
        srcIpLengthBytes = self.srcIpLength
        srcPortBytes = ByteCodec.short_to_bytes(self.srcPort)
        dstIpBytes = ByteCodec.ip_to_bytes(self.dstIp)
        dstIpLengthBytes = self.dstIpLength
        dstPortBytes = ByteCodec.short_to_bytes(self.dstPort)
        protoBytes = self.protocol
        srcMacBytes = ByteCodec.mac_to_bytes(self.srcMac)
        dstMacBytes = ByteCodec.mac_to_bytes(self.dstMac)
        statsBytes = self.statsInfo.stats_info_to_bytes()

        return flowTypeBytes + deviceIdBytes + inIntfIdBytes + outIntfIdBytes + \
               vlanIdBytes + srcIpBytes + srcIpLengthBytes + srcPortBytes + \
               dstIpBytes + dstIpLengthBytes + dstPortBytes + protoBytes + \
               srcMacBytes + dstMacBytes + statsBytes
