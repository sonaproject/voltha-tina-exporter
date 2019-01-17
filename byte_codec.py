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
import sys, binascii

class ByteCodec(object):

    byteorder='big'

    @staticmethod
    def short_to_bytes(short_val):
        return short_val.to_bytes(2, ByteCodec.byteorder)
    
    @staticmethod
    def int_to_bytes(int_val):
        return int_val.to_bytes(4, ByteCodec.byteorder)

    @staticmethod
    def long_to_bytes(long_val):
        return long_val.to_bytes(8, ByteCodec.byteorder)

    @staticmethod
    def ip_to_bytes(ip):
        ipBytes = b''
        if ip:
            ipBytes = bytes(map(int, ip.split('.')))
            # ipBytes = socket.inet_aton(ip)
        return ipBytes

    @staticmethod
    def mac_to_bytes(mac):
        macBytes = b''
        if mac:
            macBytes = binascii.unhexlify(str.encode(mac).replace(b':', b''))
        return macBytes
