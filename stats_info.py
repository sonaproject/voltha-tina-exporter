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

import time

from byte_codec import ByteCodec

class StatsInfo():
    startupTime = 0
    fstPktArrTime = 0
    lstPktOffset = 0
    prevAccBytes = 0
    prevAccPkts = 0
    currAccBytes = 0
    currAccPkts = 0
    errorPkts = 0
    dropPkts = 0
    def __init__(self):
        self.startupTime = int(round(time.time() * 1000))
        self.fstPktArrTime = int(round(time.time() * 1000))

    def stats_info_to_bytes(self):
        startupTimeBytes = ByteCodec.long_to_bytes(self.startupTime)
        fstPktArrTimeBytes = ByteCodec.long_to_bytes(self.fstPktArrTime)
        lstPktOffsetBytes = ByteCodec.int_to_bytes(self.lstPktOffset)
        prevAccPktsBytes = ByteCodec.int_to_bytes(self.prevAccPkts)
        currAccPktsBytes = ByteCodec.int_to_bytes(self.currAccPkts)
        prevAccBytesBytes = ByteCodec.long_to_bytes(self.prevAccBytes)
        currAccBytesBytes = ByteCodec.long_to_bytes(self.currAccBytes)
        errorPktsBytes = ByteCodec.short_to_bytes(self.errorPkts)
        dropPktsBytes = ByteCodec.short_to_bytes(self.dropPkts)

        return startupTimeBytes + fstPktArrTimeBytes + lstPktOffsetBytes + \
               prevAccBytesBytes + prevAccPktsBytes + currAccBytesBytes + \
               currAccPktsBytes + errorPktsBytes + dropPktsBytes
