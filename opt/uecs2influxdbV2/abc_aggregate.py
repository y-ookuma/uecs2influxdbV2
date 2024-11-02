import json
import configparser
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('uecs2influxdb.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class InfluxDBProcessor:
    def __init__(self, config_path: str, ccm_path: str):
        """
        設定ファイルとCCMファイルから初期化を行う
        
        Args:
            config_path (str): 設定ファイルのパス
            ccm_path (str): CCMファイルのパス
        """
        self.config = self._load_config(config_path)
        self.measurements = self._load_measurements(ccm_path)
        self.client = None
        self.time_ranges = [
            {"start_hour": 0, "stop_hour": 6, "prefix": "ABC_0-6"},
            {"start_hour": 6, "stop_hour": 12, "prefix": "ABC_6-12"},
            {"start_hour": 12, "stop_hour": 18, "prefix": "ABC_12-18"},
            {"start_hour": 18, "stop_hour": 24, "prefix": "ABC_18-24"},
        ]

    def _load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込む"""
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            return {
                'url': config['influx2']['url'],
                'org': config['influx2']['org'],
                'token': config['influx2']['token'],
                'bucket': config['influx2']['bucket'],
                'aggregate_bucket': "aggregate"
            }
        except Exception as e:
            logger.error(f"設定ファイルの読み込みに失敗: {e}")
            raise

    def _load_measurements(self, ccm_path: str) -> List[str]:
        """CCMファイルから測定値を読み込む"""
        try:
            with open(ccm_path, 'r') as file:
                data = json.load(file)
            return [m['measurement'] for m in data if m['savemode'] == 'abc']
        except Exception as e:
            logger.error(f"CCMファイルの読み込みに失敗: {e}")
            raise

    def connect(self):
        """InfluxDBに接続"""
        try:
            self.client = InfluxDBClient(
                url=self.config['url'],
                token=self.config['token'],
                org=self.config['org']
            )
            # 接続テスト
            self.client.ping()
            logger.info("InfluxDBに接続成功")
        except Exception as e:
            logger.error(f"InfluxDBへの接続に失敗: {e}")
            raise

    def generate_query(self, measurement: str, start_hour: int, stop_hour: int, prefix: str) -> str:
        """Fluxクエリを生成"""
        return f'''
        from(bucket: "{self.config['bucket']}")
            |> range(start: -2d, stop: -1d)
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> filter(fn: (r) => r._field == "value")
            |> filter(fn: (r) => uint(v: r._time) % uint(v: 24h) >= uint(v: {start_hour}h) and uint(v: r._time) % uint(v: 24h) < uint(v: {stop_hour}h))
            |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
            |> set(key: "_measurement", value: "{prefix}")
            |> set(key: "original_measurement", value: "{measurement}")
            |> to(bucket: "{self.config['aggregate_bucket']}", org: "{self.config['org']}")
        '''

    def process_data(self):
        """すべての測定値と時間範囲に対してクエリを実行"""
        if not self.client:
            raise RuntimeError("InfluxDBクライアントが初期化されていません")

        query_api = self.client.query_api()
        total_measurements = len(self.measurements) * len(self.time_ranges)
        processed = 0

        try:
            for measurement in self.measurements:
                for time_range in self.time_ranges:
                    try:
                        query = self.generate_query(
                            measurement=measurement,
                            start_hour=time_range["start_hour"],
                            stop_hour=time_range["stop_hour"],
                            prefix=time_range["prefix"]
                        )
                        query_api.query(org=self.config['org'], query=query)
                        processed += 1
                        logger.info(f"処理進捗: {processed}/{total_measurements} "
                                  f"({measurement}, {time_range['prefix']})")
                    except InfluxDBError as e:
                        logger.error(f"クエリ実行エラー ({measurement}, {time_range['prefix']}): {e}")
                        continue
        finally:
            if self.client:
                self.client.close()
                logger.info("InfluxDB接続を終了")

def main():
    try:
        processor = InfluxDBProcessor('uecs2influxdb.cfg', 'receive_ccm.json')
        processor.connect()
        processor.process_data()
    except Exception as e:
        logger.error(f"予期せぬエラーが発生: {e}")
        raise

if __name__ == "__main__":
    main()
