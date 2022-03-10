from(bucket: "uecs")
|> range(start: -1y)
|> filter(fn: (r) => r["_measurement"] == "wairtemp_1_2_1")
|> filter(fn: (r) => r["cloud"] == "0" and r["downsample"] == "0" )
|> filter(fn: (r) => r["_field"] == "value")
|> aggregateWindow(every: 1y, fn: last ,createEmpty: false)

