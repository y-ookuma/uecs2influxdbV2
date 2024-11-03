#!/usr/bin/python3

import os
from socket import *
import time
from datetime import datetime
import pandas as pd
import xmltodict
import json
import configparser
from typing import Dict, Set
from dataclasses import dataclass
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

@dataclass
class CCMFlags:
    """CCMフラグを管理するデータクラス"""
    flag_up: Set[str]
    flag_diff: Set[str]
    flag_max: Set[str]
    flag_abc: Set[str]

class Config:
    """設定を管理するクラス"""
    @staticmethod
    def load_config() -> tuple[CCMFlags, configparser.ConfigParser]:
        """設定ファイルを読み込む"""
        base_path = os.path.dirname(os.path.abspath(__file__))
        
        # CCM設定の読み込み
        with open(f'{base_path}/receive_ccm.json', 'r') as f:
            json_load = json.load(f)
        
        up_, diff_, max_, abc_ = [], [], [], []
        
        for data in json_load.values():
            recv_ccm = f"{data['type'].split('.')[0]}_{data['room']}_{data['region']}_{data['order']}".lower()
            
            if data["savemode"]:
                up_.append(recv_ccm)
                if data["savemode"] == "diff":
                    diff_.append(recv_ccm)
                elif data["savemode"] in ("on", "off"):
                    max_.append(recv_ccm)
                elif data["savemode"] == "abc":
                    abc_.append(recv_ccm)
        
        ccm_flags = CCMFlags(
            flag_up=set(up_),
            flag_diff=set(diff_),
            flag_max=set(max_),
            flag_abc=set(abc_)
        )
        
        # InfluxDB設定の読み込み
        config = configparser.ConfigParser()
        config.read(f'{base_path}/uecs2influxdb.cfg')
        
        return ccm_flags, config

class UECSReceiver:
    """UECSデータ受信とInfluxDBへの書き込みを行うクラス"""
    def __init__(self, config: configparser.ConfigParser):
        self.setup_udp()
        self.setup_influxdb(config)
    
    def setup_udp(self, port: int = 16520):
        """UDPソケットの設定"""
        self.udp_socket = socket(AF_INET, SOCK_DGRAM)
        self.udp_socket.bind(("", port))
        self.BUFSIZE = 512
    
    def setup_influxdb(self, config: configparser.ConfigParser):
        """InfluxDB接続の設定"""
        self.bucket = config["influx2"]["bucket"]
        self.client = InfluxDBClient.from_config_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uecs2influxdb.cfg')
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
    
    def process_ccm_data(self, ccm_data: bytes) -> Dict:
        """CCMデータの解析"""
        dictionary = xmltodict.parse(ccm_data)
        json_data = json.loads(json.dumps(dictionary).replace('@', '').replace('#', ''))
        
        data = json_data["UECS"]["DATA"]
        measurement = f"{data['type'].split('.')[0]}_{data['room']}_{data['region']}_{data['order']}".lower()
        
        return {
            "measurement": measurement,
            "value": float(data["text"]),
            "priority": data["priority"]
        }
    
    async def get_last_value(self, measurement: str) -> float:
        """過去のデータを取得"""
        query = f'''
            from(bucket: "{self.bucket}")
                |> range(start: -1y)
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => r["cloud"] == "0" and r["downsample"] == "0")
                |> filter(fn: (r) => r["_field"] == "value")
                |> last()
        '''
        
        tables = self.query_api.query(query)
        for table in tables:
            for record in table.records:
                return float(record["_value"])
        return 0.0
    
    def write_to_influxdb(self, data: Dict):
        """InfluxDBへの書き込み"""
        write_options = WriteOptions(
            batch_size=500,
            flush_interval=10_000,
            jitter_interval=2_000,
            retry_interval=5_000,
            max_retries=5,
            max_retry_delay=30_000,
            exponential_base=2
        )
        
        with self.client.write_api(write_options=write_options) as write_client:
            influx_data = {
                "measurement": data["measurement"],
                "tags": {"cloud": "0", "downsample": "0","priority":data["priority"]},
                "fields": {"value": data["value"]}
            }
            write_client.write(bucket=self.bucket, record=influx_data)
    
    async def receive(self, ccm_flags: CCMFlags, debug: bool = False, debug_sec: float = None):
        """UECSデータの受信とデータ処理"""
        start_time = time.time()
        debug_count = 0
        
        while True:
            ccm_data, addr = self.udp_socket.recvfrom(self.BUFSIZE)
            if debug:
                print(f"Received: {ccm_data.decode()}, from: {addr}")
                debug_count += 1
            
            try:
                data = self.process_ccm_data(ccm_data)
                
                if data["measurement"] not in ccm_flags.flag_up:
                    continue
                
                # 差分計算
                if data["measurement"] in ccm_flags.flag_diff:
                    last_value = await self.get_last_value(data["measurement"])
                    data["value"] = abs(data["value"] - last_value)
                
                # 四捨五入
                if data["measurement"] in ccm_flags.flag_max:
                    data["value"] = round(data["value"])
                
                self.write_to_influxdb(data)
                
            except Exception as e:
                print(f"Error processing data: {e}")
            
            if debug_sec and (time.time() - start_time) >= debug_sec:
                print(f"Debug time: {time.time() - start_time:.2f}s, Messages: {debug_count}")
                break

def main():
    """メイン処理"""
    try:
        ccm_flags, config = Config.load_config()
        receiver = UECSReceiver(config)
        
        import asyncio
        asyncio.run(receiver.receive(
            ccm_flags,
            debug=True,
            debug_sec=None  # デバッグ時間を指定する場合は数値を設定
        ))
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        receiver.client.close()

if __name__ == "__main__":
    main()
