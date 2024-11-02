//---------------------------------------------------------------------- 
// 2024.11.02 Downsampling TASK
// InfluxdbのTASKでDownsamplingを実施するスクリプトです。
//    bucketと org は適宜編集してください。
//---------------------------------------------------------------------- 

// task名 1時間毎に実施
option task = {name: "aggregate(24ｈ)", every: 1h}
// 1日前から現在まで10分間隔で。
option v = {timeRangeStart: -1d, timeRangeStop: now(), windowPeriod: 10m}

// bucket: "uecs"→"aggregate"にダウンサンプリング
from(bucket: "uecs")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    //    この行を削除することによりbucket全体のmeasurementsをダウンサンプリングされる。
    //    |> filter(fn: (r) => r["_measurement"] == "k_sht31temp_1_5_1")
    |> filter(fn: (r) => r["_field"] == "value")
    |> filter(fn: (r) => r["cloud"] == "0")
    |> filter(fn: (r) => r["downsample"] == "0")
    |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    |> to(bucket: "aggregate", org: "ookumafarm")
