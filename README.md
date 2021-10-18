# OpenSearch KNN Hammer

## Overview
Simple tool to ingest random data and run random queries into OpenSearch for the k-NN plugin

## Ingest
```
python3 hammer.py {URL} {Security Enabled: true/false} ingest {INDEX_NAME} {FIELD_NAME} {DIMENSION} {NUMBER_OF_VECTORS}
```

## Search
```
python3 hammer.py {URL} {Security Enabled: true/false} search {INDEX_NAME} {FIELD_NAME} {DIMENSION} {K} {SIZE} {NUMBER_OF_QUERIES}
```
