import os
from datetime import timedelta
import json
import logging

# Import Quixstreams for windowing operations
from quixstreams import Application
from quixstreams.dataframe.windows import Mean, Max, Min, Count, Sum

# Import Azure Data Explorer (KUSTO) libraries
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    BlobDescriptor,
    FileDescriptor,
    IngestionProperties,
    IngestionStatus,
    KustoStreamingIngestClient,
    ManagedStreamingIngestClient,
    QueuedIngestClient,
    StreamDescriptor,
    ReportLevel,
)


authority_id = ""
KUSTO_CLUSTER =  ""
KUSTO_DATABASE = "StreamData"
KUSTO_TABLE = "MyStockEvents"
client_id = ''
client_secret = ''
kustoOptions = {"kustoCluster": KUSTO_CLUSTER, "kustoDatabase" :KUSTO_DATABASE, "kustoTable" : KUSTO_TABLE }
cluster = f"https://ingest-{KUSTO_CLUSTER}.kusto.windows.net/"

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)

client = QueuedIngestClient(kcsb)
