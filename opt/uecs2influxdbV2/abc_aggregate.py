//----------------------------------------------------------------------
// 2024.11.02 Aggregate TASK
// 
//  receive_ccm.json を参照して
//    measurement の savemode=abc の場合、6時間毎に集計する    
//  Exsample：
//      集計先：buckt uecs  / measurement k_sht31temp_1_5_1
//      格納先：buckt aggregate
//          時間帯          measurement      tag
//           0-6時      -->   ABC_0-6      k_sht31temp_1_5_1
//           6-12時     -->   ABC_6-12     k_sht31temp_1_5_1
//          12-18時     -->   ABC_12-18    k_sht31temp_1_5_1
//          18-24時     -->   ABC_18-24    k_sht31temp_1_5_1
//----------------------------------------------------------------------
import json
import configparser
import logging,os
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError

# ロギングの設定
log_dir = 'log' 
os.makedirs(log_dir, exist_ok=True) 
log_filename = os.path.join(log_dir, f'uecs2influxdb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig( 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    handlers=[ logging.FileHandler(log_filename),
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
                'aggregate_bucket':config['influx2']['aggregate_bucket']
            }
        except Exception as e:
            logger.error(f"設定ファイルの読み込みに失敗: {e}")
            raise

    def _load_measurements(self, ccm_path: str) -> List[str]:
        """CCMファイルから測定値を読み込む"""
        try:
            with open(ccm_path, 'r') as file:
                data = json.load(file)
            return [key for key, value in data.items() if value.get('savemode') == 'abc']
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

    def get_latest_data_date(self, measurement: str) -> Optional[datetime]:
        """データベースの最新のデータ日付を取得"""
        query = f'''
        from(bucket: "{self.config['bucket']}")
            |> range(start: -30d)
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> filter(fn: (r) => r._field == "value")
            |> last()
        '''
        try:
            result = self.client.query_api().query(org=self.config['org'], query=query)
            if result and len(result) > 0 and len(result[0].records) > 0:
                return result[0].records[0].get_time()
            return None
        except Exception as e:
            logger.error(f"最新データ日付の取得に失敗 ({measurement}): {e}")
            return None

    def get_check_periods(self, latest_date: datetime) -> List[Dict]:
        """最新のデータ日付から集計期間のリストを生成"""
        today = datetime.now().date()
        latest_date = latest_date.date()
        days_diff = (today - latest_date).days

        # 最新データから1日前までの期間を1日ごとに生成
        periods = []
        for i in range(days_diff, 1, -1):  # 2日前まで（1日前は除く）
            periods.append({
                "days_back": i,
                "days_range": 1  # 1日ごとの集計
            })
        return periods

    def check_data_exists(self, measurement: str, days_back: int) -> bool:
        """指定した日のデータが存在するかチェック"""
        query = f'''
        from(bucket: "{self.config['aggregate_bucket']}")
            |> range(start: -{days_back}d, stop: -{days_back-1}d)
            |> filter(fn: (r) => r.original_measurement == "{measurement}")
            |> count()
        '''
        try:
            result = self.client.query_api().query(org=self.config['org'], query=query)
            return len(result) > 0 and len(result[0].records) > 0
        except Exception as e:
            logger.warning(f"データ存在チェック中にエラー ({measurement}, {days_back}日前): {e}")
            return False

    def generate_query(self, measurement: str, start_hour: int, stop_hour: int, prefix: str, 
                      days_back: int) -> str:
        """Fluxクエリを生成"""
        return f'''
        from(bucket: "{self.config['bucket']}")
            |> range(start: -{days_back}d, stop: -{days_back-1}d)
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

        try:
            for measurement in self.measurements:
                # 最新のデータ日付を取得
                latest_date = self.get_latest_data_date(measurement)
                if not latest_date:
                    logger.warning(f"最新データが見つかりません: {measurement}")
                    continue

                # 集計期間を動的に生成
                check_periods = self.get_check_periods(latest_date)
                total_combinations = len(self.time_ranges) * len(check_periods)
                processed = 0

                logger.info(f"処理開始: {measurement} "
                          f"(最新データ日付: {latest_date.strftime('%Y-%m-%d')})")

                for period in check_periods:
                    days_back = period["days_back"]
                    
                    # データの存在をチェック
                    if self.check_data_exists(measurement, days_back):
                        logger.info(f"{days_back}日前のデータは既に存在します: {measurement}")
                        continue

                    # 各時間帯の集計を実行
                    for time_range in self.time_ranges:
                        try:
                            query = self.generate_query(
                                measurement=measurement,
                                start_hour=time_range["start_hour"],
                                stop_hour=time_range["stop_hour"],
                                prefix=time_range["prefix"],
                                days_back=days_back
                            )
                            query_api.query(org=self.config['org'], query=query)
                            processed += 1
                            logger.info(f"処理進捗: {processed}/{total_combinations} "
                                      f"({measurement}, {time_range['prefix']}, "
                                      f"{days_back}日前)")
                        except InfluxDBError as e:
                            logger.error(f"クエリ実行エラー "
                                       f"({measurement}, {time_range['prefix']}, "
                                       f"{days_back}日前): {e}")
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
