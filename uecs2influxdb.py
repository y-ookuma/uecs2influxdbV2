#----------------------------------------------------------------------*/influxdb_client
# 2022.03.11 UECS 
# sudo apt-get install python3-pip -y
# sudo apt-get install python3-pandas -y
# sudo apt-get install python3-influxdb -y
# sudo pip3 install influxdb-client
# sudo pip3 install xmltodict
#----------------------------------------------------------------------*/
#!/usr/bin/python3

from socket import *
import time as t
import pandas as pd
import xmltodict,json,os,configparser
from multiprocessing import Process
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions

## 初期設定
class Initialset():
    def parm_set():
        # 受信設定ファイルを読み込む。
        ccm_json = os.path.dirname(os.path.abspath(__file__)) + '/receive_ccm.json' #CNF
        json_open = open(ccm_json, 'r')
        json_load = json.load(json_open)
        # 保存用種別を4つ作成
        up_ ,diff_ ,max_ ,abc_=[],[],[],[]

        for k in json_load.keys():
            recv_ccm = json_load[k]["type"].split(".")[0]
            recv_ccm += "_" + json_load[k]["room"]
            recv_ccm += "_" + json_load[k]["region"]
            recv_ccm += "_" + json_load[k]["order"]
            recv_ccm = recv_ccm.lower()                   # 小文字に変換
            if json_load[k]["savemode"] not in (None,""):
                up_.append(recv_ccm)
            if json_load[k]["savemode"] == "diff":
                diff_.append(recv_ccm)
            elif json_load[k]["savemode"] in ("on","off"):
                max_.append(recv_ccm)
            elif json_load[k]["savemode"] =="abc":
                abc_.append(recv_ccm)

        recv_ccm = {"flag_up":set(up_) ,"flag_diff":set(diff_) ,  # 集合型に変換
                    "flag_max":set(max_) ,"flag_abc":set(abc_)}

        # read config
        filepath = os.path.dirname(os.path.abspath(__file__))+ '/uecs2influxdb.cfg'
        config = configparser.ConfigParser()
        config.read(filepath)

        return recv_ccm,config

## UDP受信クラス
class udprecv():
    def __init__(self,config):

        SrcIP = ""                                               # 受信元IP
        SrcPort = 16520                                          # 受信元ポート番号
        self.SrcAddr = (SrcIP, SrcPort)                          # アドレスをtupleに格納

        self.BUFSIZE = 512                                       # バッファサイズ指定
        self.udpServSock = socket(AF_INET, SOCK_DGRAM)           # ソケット作成
        self.udpServSock.bind(self.SrcAddr)                      # 受信元アドレスでバインド

        # influxdb パラメータ
        param = os.path.dirname(os.path.abspath(__file__))+ '/uecs2influxdb.cfg'
        self.bucket = config["influx2"]["bucket"]
        self.client = InfluxDBClient.from_config_file(param)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def recv(self,debug=False,debug_sec=None,ccm_list=[]):
        if debug_sec is not None:    # debug_sec が指定されている場合
            start=t.time()
            debug_list=[]
            print("ccm_list",ccm_list)

        while True:                                              # 常に受信待ち
            ccm, addr = self.udpServSock.recvfrom(self.BUFSIZE)  # 受信
#            print(ccm.decode(), addr)                            # 受信データと送信アドレス表示

            p = Process(target=self.save_df, args=(debug,ccm,ccm_list))        # マルチプロセス化でDB処理などを実行する
            p.start()
            if p.join(5) is None:
                p.terminate()
            # デバックモード 
            if debug: 
                print(ccm.decode(), addr)                           # 受信データと送信アドレス表示
                if debug_sec is not None:                           # 秒 指定がある場合
                    end=t.time()
                    debug_list.append(ccm)
                    print("Main process ID:",os.getppid())
                    if end-start>=debug_sec:
                        print("debug_time:",round(end-start,2),"ExecCount:",len(debug_list))
                        break;

    # DB保存処理
    def save_df(self,debug,ccm,ccm_list):
        dictionary = xmltodict.parse(ccm)                            # xmlを辞書型へ変換
        json_string = json.dumps(dictionary)                         # json形式のstring
        json_string = json_string.replace('@', '').replace('#', '')  # 「#や@」 をreplace
        json_object = json.loads(json_string)                        # Stringを再度json形式で読み込む

        measurement = json_object["UECS"]["DATA"]["type"].split(".")[0]
        measurement += "_" + json_object["UECS"]["DATA"]["room"]
        measurement += "_" + json_object["UECS"]["DATA"]["region"]
        measurement += "_" + json_object["UECS"]["DATA"]["order"]
        measurement = measurement.lower()                             # 小文字に変換
        datetime    = pd.Timestamp.utcnow()
        val         = float(json_object["UECS"]["DATA"]["text"])*1.0
        priority    = json_object["UECS"]["DATA"]["priority"]

        # 保存用のCCMでない場合、EXITする
        if measurement not in ccm_list["flag_up"]:
            exit(0)

        # 前回のデータと今回のデータの差分処理
        if measurement in ccm_list["flag_diff"] :
            p = {"_bucket": self.bucket ,"_measurement": measurement}
            q ='''
                  from(bucket: _bucket)
                  |> range(start: -1y)
                  |> filter(fn: (r) => r["_measurement"] == _measurement)
                  |> filter(fn: (r) => r["cloud"] == "0" and r["downsample"] == "0" )
                  |> filter(fn: (r) => r["_field"] == "value")
                  |> aggregateWindow(every: 1y, fn: last, createEmpty: false)
               '''
            tables = self.query_api.query(query=q, params=p)

            last_val=None
            for table in tables:
                for record in table.records:
                    last_val = float(record["_value"])

            if last_val in (None,0): #前回のデータがなければ、今回のデータを使う
                last_val= val
            val = abs(val - last_val) #絶対値

        # 四捨五入する 0,1 のデータを対象とする
        if measurement in ccm_list["flag_max"]:
            val = round(val) * 1.0

        # ↓↓↓  influxDB用整形
        data = {"measurement": measurement, 
                 "tags": {"cloud": "0","downsample": "0"} ,
                 "fields": {"value": val}}
        # ↑↑↑  influxDB用整形
        p = Process(target=self.influx_write, args=(debug,data))        # マルチプロセス化でDB処理などを実行する
        p.start()
        if p.join(5) is None:
            p.terminate()

        if debug:
            print("Sub_process ID:",os.getppid())

    def influx_write(self,debug,data):
        with self.client.write_api(write_options=WriteOptions(batch_size=500,
                                                      flush_interval=10_000,
                                                      jitter_interval=2_000,
                                                      retry_interval=5_000,
                                                      max_retries=5,
                                                      max_retry_delay=30_000,
                                                      exponential_base=2)) as _write_client:

            _write_client.write(bucket=self.bucket, record=data)


#-------------------------------------------------------#
# Main 処理
#-------------------------------------------------------#

# パラメータセット
ccm_list,config=Initialset.parm_set()

#UECS受信
udp = udprecv(config)     # クラス呼び出し
#udp.recv(debug=True,debug_sec=30,ccm_list=ccm_list)     # デバックモード　10秒間
udp.recv(debug=True,ccm_list=ccm_list)                 # 本番処理　debug_secを指定しない
