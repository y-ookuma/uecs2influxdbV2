from socket import *
import json,os,subprocess
import time as t
import xmltodict
import pandas as pd

def read_ccm_json(ccm_json):
    ccm_list=[]
    df = pd.read_json(ccm_json ) 

    json_key_list=[]
    for c in df.columns.values:
        json_key  = df.loc['type', c] .split(".")[0].lower()   #df.loc[インデックス名, カラム名]
        json_key += "_"+ df.loc['room', c]
        json_key += "_"+ df.loc['region', c]
        json_key += "_"+ df.loc['order', c]
        json_key_list.append(json_key)

    df.loc["json_key"]=json_key_list  
    df=df.transpose()   #行と列入れ替え

    return set(json_key_list),df


def kill_uecs_proc():
    print('-------------------------------------')
    print(' receive_ccm.json 作成処理')
    print('-------------------------------------')
    print('')
    print('Please wait a few minuite............')
    print('')
    print('sudo systemctl stop uecs2influxdb.service')
    print('')
    cmd = "sudo systemctl stop uecs2influxdb.service"
    subprocess.call( cmd, shell=True )
    print('kill uecs socket............')
    print('')
    cmd = "ps -aux |grep uecs2influxdb|awk \'{print \"sudo kill\",$2}\' | sh"
    subprocess.call( cmd, shell=True )

def start_uecs_proc():
    print('sudo systemctl start uecs2influxdb.service')
    print('')
    cmd = "sudo systemctl start uecs2influxdb.service"
    subprocess.call( cmd, shell=True )
    print('------------------------------------------------------')
    print(' 正常に uecs2influxdb が動作していることを確認ください')
    print('------------------------------------------------------')

def capture_ccm(sec_time=50):
    #jsonファイルを読み込む
    ccm_json = os.path.dirname(os.path.abspath(__file__)) + '/receive_ccm.json' #CNF
    json_key_list,df_json=set([]),None
    if os.path.exists(ccm_json):
        json_key_list,df_json = read_ccm_json(ccm_json)        

    print('-------------------------------------')
    print(' 以下のCCMを取り込んでいます.........')
    print(' '+ str(sec_time)  +'秒間かかります...................')
    print('-------------------------------------')
    #ccm capture start 
    HOST = ''
    PORT = 16520
    s =socket(AF_INET,SOCK_DGRAM)
    s.bind((HOST, PORT))

    start=t.time()
    end=t.time()
    add_ccm=[]
    while end - start < sec_time:
        end=t.time()
        msg, address = s.recvfrom(512)

        dictionary = xmltodict.parse(msg)                            # xmlを辞書型へ変換
        json_string = json.dumps(dictionary)                         # json形式のstring
        json_string = json_string.replace('@', '').replace('#', '')  # 「#や@」 をreplace
        json_object = json.loads(json_string)                        # Stringを再度json形式で読み込む

        ccm_key= (json_object["UECS"]["DATA"]["type"]).split(".")[0].lower() \
                    +"_"+ json_object["UECS"]["DATA"]["room"] \
                    +"_"+ json_object["UECS"]["DATA"]["region"] \
                    +"_"+ json_object["UECS"]["DATA"]["order"]

        if ccm_key not in json_key_list:
            add_ccm.append({
                     "type":       json_object["UECS"]["DATA"]["type"] #.split(".")[0].lower()
                    ,"room":       json_object["UECS"]["DATA"]["room"]
                    ,"region":     json_object["UECS"]["DATA"]["region"]
                    ,"order":      json_object["UECS"]["DATA"]["order"]
                    ,"sendlevel":  ""
                    ,"savemode":   ""
                    ,"json_key":  ccm_key
                    })

            json_key_list.add(ccm_key)

            print("【" + str(len(add_ccm)) + "件】"
                    , " 残り:"+str(sec_time-round(end - start,1))+"秒 "
                    ,ccm_key)

    # キャプチャしたデータをDataframe化
    df_ccm = pd.DataFrame(add_ccm ,columns= ['type','room','region','order','sendlevel','savemode','json_key'])

    # CCM受信の場合は、json_key をindexにする json_keyはなくなる
    df_ccm = df_ccm.set_index('json_key')

    # receive_ccm.json と CCMキャプチャとの結合
    if df_json is not None:
        df = df_json.append(df_ccm)
    else:
        df = df_ccm
    # ソート
    df.sort_values(['room','region','order','type'],ignore_index=False, inplace = True)  
    if 'json_key' in df.columns :
        df.drop(columns='json_key', inplace=True) #json_key削除

    output_json = df.to_json(orient="index",force_ascii=False)   #形式を指定: 全角文字（日本語）などのUnicodeエスケープ指定
    parsed_output_json = json.loads(output_json)

    if len(df_ccm)>0: #変更があれば
        path = os.path.dirname(os.path.abspath(__file__)) + '/receive_ccm.json' #CCMのデータをreceive_ccm.jsonに保存する
        with open(path, 'w') as f:
            json.dump(parsed_output_json, f,indent=4, ensure_ascii=False)

        print('-------------------------------------------------')
        print(' receive_ccm.json を再作成完了しました。.........')
        print('-------------------------------------------------')
    else:
        print('-------------------------------------------------')
        print(' receive_ccm.json の変更はありません。.........')
        print('-------------------------------------------------')


kill_uecs_proc()
capture_ccm(sec_time=60)  # 60秒間データ受信する
start_uecs_proc()
