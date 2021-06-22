query = """
from(bucket: "trading")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "matches")
  |> filter(fn: (r) => r["exchange"] == "coinbasepro")
  |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "size")
  |> pivot(rowKey: ["_time", "market", "exchange", "side"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({r with _value: r["size"] * r["price"], _field: "quote_volume"}))
  |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
  |> pivot(rowKey: ["_time", "market", "exchange"], columnKey: ["side"], valueColumn: "_value")
  |> map(fn: (r) => ({r with _value: r["buy"] * 100.0 / (r["buy"] + r["sell"])}))
  |> yield(name: "_value")
"""
