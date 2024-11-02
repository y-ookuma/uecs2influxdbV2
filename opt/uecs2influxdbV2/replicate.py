import configparser
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Set
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

def setup_logging() -> None:
    """ロギングの設定"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )

def load_config(config_path: str) -> configparser.ConfigParser:
    """設定ファイルの読み込みと検証"""
    config = configparser.ConfigParser()
    try:
        if not config.read(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        required_sections = ['influx2', 'influxdb_cloud']
        required_params = {
            'influx2': ['host_name', 'port', 'user', 'pass', 'database', 'bucket', 'aggregate_bucket'],
            'influxdb_cloud': ['host_name', 'port', 'user', 'pass', 'database']
        }
        
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing section: {section}")
            for param in required_params[section]:
                if param not in config[section]:
                    raise ValueError(f"Missing parameter: {param} in section {section}")
        
        return config
    except Exception as e:
        logging.error(f"Failed to load configuration: {str(e)}")
        raise

def load_measurement_filter(json_path: str) -> Set[str]:
    """
    receive_ccm.jsonを読み込み、移行対象のmeasurementを特定する
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            ccm_data = json.load(f)
        
        # savemodeが有効な値（空でない、nullでない）のキーを抽出
        valid_measurements = {
            key for key, value in ccm_data.items()
            if value.get('savemode') and value['savemode'].strip()
        }
        
        logging.info(f"Loaded {len(valid_measurements)} valid measurements from {json_path}")
        return valid_measurements
    
    except Exception as e:
        logging.error(f"Failed to load measurement filter: {str(e)}")
        raise

def connect_to_database(config: Dict[str, str], is_source: bool = True) -> InfluxDBClient:
    """データベースへの接続"""
    db_type = "source" if is_source else "target"
    try:
        client = InfluxDBClient(
            host=config['host_name'],
            port=int(config['port']),
            username=config['user'],
            password=config['pass'],
            database=config['database']
        )
        client.ping()  # 接続テスト
        logging.info(f"Successfully connected to {db_type} database")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to {db_type} database: {str(e)}")
        raise

def ensure_target_database(client: InfluxDBClient, database: str) -> None:
    """ターゲットデータベースの存在確認と作成"""
    try:
        databases = client.get_list_database()
        if not any(db['name'] == database for db in databases):
            client.create_database(database)
            logging.info(f"Created target database: {database}")
    except Exception as e:
        logging.error(f"Failed to ensure target database: {str(e)}")
        raise

def process_measurement(
    source_client: InfluxDBClient,
    target_client: InfluxDBClient,
    measurement_name: str,
    source_bucket: str,
    batch_size: int = 1000
) -> None:
    """測定値の処理とデータ移行"""
    try:
        results = source_client.query(f'SELECT * FROM {measurement_name}')
        points = list(results.get_points())
        
        if not points:
            logging.info(f"No points found for measurement: {measurement_name} in bucket {source_bucket}")
            return

        # フィールドの特定
        fields = results.raw['series'][0]['columns']
        field_key = 'value' if 'value' in fields else fields[1]

        # 重複チェック用のデータ取得
        existing_query = f'SELECT * FROM {measurement_name} WHERE time > now() - 30d'
        existing_points = target_client.query(existing_query)
        existing_points_set = set(
            (point['time'], point[field_key])
            for point in existing_points.get_points()
        )

        # 新しいポイントの作成
        new_points = []
        for point in points:
            if (point['time'], point[field_key]) not in existing_points_set:
                new_point = {
                    "measurement": measurement_name,
                    "tags": {
                        **point.get('tags', {}),
                        'source_bucket': source_bucket
                    },
                    "time": point['time'],
                    "fields": {field_key: point[field_key]}
                }
                new_points.append(new_point)

            # バッチサイズに達したら書き込み
            if len(new_points) >= batch_size:
                target_client.write_points(new_points)
                logging.info(f"Wrote {len(new_points)} points for {measurement_name} from {source_bucket}")
                new_points = []

        # 残りのポイントを書き込み
        if new_points:
            target_client.write_points(new_points)
            logging.info(f"Wrote final {len(new_points)} points for {measurement_name} from {source_bucket}")

    except Exception as e:
        logging.error(f"Error processing measurement {measurement_name} from {source_bucket}: {str(e)}")
        raise

def process_bucket(
    source_client: InfluxDBClient,
    target_client: InfluxDBClient,
    bucket: str,
    valid_measurements: Set[str]
) -> None:
    """バケット単位でのデータ処理"""
    try:
        # データベースの切り替え
        source_client.switch_database(bucket)
        
        # 測定値の取得と処理
        measurements = source_client.query("SHOW MEASUREMENTS").get_points()
        for measurement in measurements:
            measurement_name = measurement["name"]
            # 有効なmeasurementのみを処理
            if measurement_name in valid_measurements:
                logging.info(f"Processing valid measurement: {measurement_name} from bucket: {bucket}")
                process_measurement(source_client, target_client, measurement_name, bucket)
            else:
                logging.info(f"Skipping measurement {measurement_name} (not in valid measurements list)")
            
    except Exception as e:
        logging.error(f"Error processing bucket {bucket}: {str(e)}")
        raise

def main() -> None:
    """メイン処理"""
    setup_logging()
    try:
        # 設定の読み込み
        config = load_config('uecs2influxdb.cfg')
        valid_measurements = load_measurement_filter('receive_ccm.json')
        
        source_config = config['influx2']
        target_config = config['influxdb_cloud']

        # データベース接続
        source_client = connect_to_database(source_config, is_source=True)
        target_client = connect_to_database(target_config, is_source=False)

        # ターゲットデータベースの確認
        ensure_target_database(target_client, target_config['database'])
        target_client.switch_database(target_config['database'])

        # バケットの処理
        buckets = [source_config['bucket'], source_config['aggregate_bucket']]
        for bucket in buckets:
            logging.info(f"Starting processing bucket: {bucket}")
            process_bucket(source_client, target_client, bucket, valid_measurements)
            logging.info(f"Completed processing bucket: {bucket}")

        logging.info("Migration completed successfully")

    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        raise
    finally:
        # クライアントの接続を閉じる
        if 'source_client' in locals():
            source_client.close()
        if 'target_client' in locals():
            target_client.close()

if __name__ == "__main__":
    main()
