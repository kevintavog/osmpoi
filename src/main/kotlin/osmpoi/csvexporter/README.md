Old queries:
- Everything in given bounds
```
  """
  {
  "size": 10,
  "sort": [
  {
  "dateCreated": {
  "order": "asc",
  "format": "strict_date_optional_time_nanos",
  "numeric_type": "date_nanos"
  }
  },
  {"_shard_doc": "desc"}
  ],
  "query": {
  "bool": {
  "must": {
  "query_string": {
  "query": "sites:*",
  "default_field": "*"
  }
  },
  "filter": {
  "geo_bounding_box": {
  "location": {
  "top_left": {
  "lat": 47.830000000000005,
  "lon": -122.45999990000001
  },
  "bottom_right": {
  "lat": 47.39,
  "lon": -122.00999990000001
  }
  }
  }
  }
  }
  }
  }
  """.trimIndent()
```