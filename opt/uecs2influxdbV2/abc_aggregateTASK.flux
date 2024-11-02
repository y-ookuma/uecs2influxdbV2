//----------------------------------------------------------------------
// 2024.11.02 Aggregate TASK
// InfluxdbのTASKで時間帯毎に平均値を集計するスクリプトです。
//    bucketと org は適宜編集してください。
//
//  Exsample：
//      集計先：buckt uecs  / measurement k_sht31temp_1_5_1
//      格納先：buckt aggregate
//          時間帯          measurement      tag
//           0-6時      -->   ABC_0-6      k_sht31temp_1_5_1
//           6-12時     -->   ABC_6-12     k_sht31temp_1_5_1
//          12-18時     -->   ABC_12-18    k_sht31temp_1_5_1
//          18-24時     -->   ABC_18-24    k_sht31temp_1_5_1
//----------------------------------------------------------------------
// task名 1日毎に実施
option task = {name: "Daily Time Range Aggregation", every: 24h}
// 1日前のデータに限る
option v = {timeRangeStart: -2d, timeRangeStop: -1d}

// 0-6時のデータを集計して保存
from(bucket: "uecs")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "k_sht31temp_1_5_1")
    |> filter(fn: (r) => r._field == "value")
    |> filter(fn: (r) => uint(v: r._time) % uint(v: 24h) < uint(v: 6h))
    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "ABC_0-6")
    |> set(key: "original_measurement", value: "k_sht31temp_1_5_1")
    |> to(bucket: "aggregate", org: "ookumafarm")

// 6-12時のデータを集計して保存
from(bucket: "uecs")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "k_sht31temp_1_5_1")
    |> filter(fn: (r) => r._field == "value")
    |> filter(
        fn: (r) =>
            uint(v: r._time) % uint(v: 24h) >= uint(v: 6h) and uint(v: r._time) % uint(v: 24h)
                <
                uint(v: 12h),
    )
    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "ABC_6-12")
    |> set(key: "original_measurement", value: "k_sht31temp_1_5_1")
    |> to(bucket: "aggregate", org: "ookumafarm")

// 12-18時のデータを集計して保存
from(bucket: "uecs")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "k_sht31temp_1_5_1")
    |> filter(fn: (r) => r._field == "value")
    |> filter(
        fn: (r) =>
            uint(v: r._time) % uint(v: 24h) >= uint(v: 12h) and uint(v: r._time) % uint(v: 24h)
                <
                uint(v: 18h),
    )
    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "ABC_12-18")
    |> set(key: "original_measurement", value: "k_sht31temp_1_5_1")
    |> to(bucket: "aggregate", org: "ookumafarm")

// 18-24時のデータを集計して保存
from(bucket: "uecs")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "k_sht31temp_1_5_1")
    |> filter(fn: (r) => r._field == "value")
    |> filter(
        fn: (r) =>
            uint(v: r._time) % uint(v: 24h) >= uint(v: 18h) and uint(v: r._time) % uint(v: 24h)
                <
                uint(v: 24h),
    )
    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "ABC_18-24")
    |> set(key: "original_measurement", value: "k_sht31temp_1_5_1")
    |> to(bucket: "aggregate", org: "ookumafarm")
