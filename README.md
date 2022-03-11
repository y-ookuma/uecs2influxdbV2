# uecs2influxdbV2
Influxdb Ver2.x Support 
[UECS(japanese greenhouse IOT Resolution)](https://uecs.jp/)　to [Influxdb](https://www.influxdata.com/).  
UECS ccmデータをInfluxdb2.xに格納します。  
これにより[Grafana](https://grafana.com/)や[telegraf](https://docs.influxdata.com/telegraf/)を使ってデータベースをリアルタイムに可視化することが可能になります。  
[UECS通信規約](https://uecs.jp/uecs/kiyaku/UECSStandard100_E10.pdf)を確認ください。


### 動作環境
Raspberry Pi4 
OS:Raspberry Pi OS (64-bit)  
MicroSD Card 16G or more / class10 / MLC  
python3.9

### Install
[インストール方法](https://github.com/y-ookuma/uecs2influxdb/wiki)を参照ください。  

### UECS 通信サンプル

```
<?xml version="1.0"?> 
<UECS ver="1.00-E10"> 
<DATA type="SoilTemp.mIC" room="1" region="1" order="1" priority="15">23.0</DATA> 
<IP>192.168.1.64</IP> 
</UECS>
```

### Influxdb格納サンプル

1. measurement名

   type＝"."(ドット)より左側のみ 小文字であり大文字を使用しない。

   room,region,orderを"_"（アンダースコア）で繋ぎます。

　　measurement名：**soiltemp_1_1_1**

3. Tag

   Cloudは、Cloudのストレージと連携した場合、”1”を付与。それ以外"0"  
   CloudストレージにUPした後、同様のデータにCloudに"1"を付与したデータを格納。その後、当該データを削除。

   DownSampleは、ダウンサンプリングした場合、”1”を付与。それ以外"0"  
   ダウンサンプリング実施した後、同様のデータのDownSampleに”1”が付与されたデータを格納。24時間経過後、DownSample”0”のデータは削除予定とします。

|          |                            | Fields   | Tag      | Tag   | Tag        |
| -------- | -------------------------- | ----- | -------- | ----- | ---------- |
| カラム名 | datetime                   | Value | Priority | Cloud | DownSample |
| 1行目      | 2021-11-20 20:19:53.776606 | 23.0  | 15       | 0     | 0          |
|          |                            |       |          |       |            |
|          |                            |       |          |       |            |


### [receive_ccm.json](https://github.com/y-ookuma/uecs2influxdb/blob/main/receive_ccm.json)

1. receive_ccm.jsonに記述済のCCM情報をすべてIfluxdbに格納します。

2. savemodeについて

   ”1”　・・・　null値でない(1でなくてもよい)場合、DBに格納します。  
   "diff"　・・・　前回のデータとの差分を絶対値として格納します。  
   ”on”　・・・　0，1のみ格納します。  
   ””　・・・　null値や空値の場合は、DBに格納しません。  

```
  "トマトハウス 気温": {
    "type": "inair_sht31temp.cMC",
    "room": "1",
    "region": "4",
    "order": "1",
    "sendlevel":"A-10S-0",
    "savemode":"1"
  }
```



### [uecs2influxdb.cfg](https://github.com/y-ookuma/uecs2influxdb/blob/main/uecs2influxdb.cfg)

uecs2influxdb.cfgにＩｎｆｌｕｘＤＢの情報を記述します。

```
[influx2]
url=http://localhost:8086
org=
token=
timeout=6000
verify_ssl=False
bucket=uecs
```


