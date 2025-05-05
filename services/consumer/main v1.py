import time
import json
import os
import io  # Added for StringIO
from datetime import datetime
import logging

# Import Quixstreams for data processing
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

# Import Azure KUSTO libraries
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add table or database level permission for Service principle using below commands
# .show database
#.add database StreamData admins ('aadapp=Serive Principle Client ID;Tenant ID') 'Service Principal'
# #.create table MyStockEvents (product_ids:string , price:dynamic ,quantity:long,timestamp:datetime )
#.alter table MyStockEvents policy streamingingestion enable


# KUSTO configuration
KUSTO_CLUSTER = ""
KUSTO_DATABASE = "StreamData"
KUSTO_TABLE = "MyStockEvents"
KUSTO_CLIENT_ID = ''
KUSTO_CLIENT_SECRET = ''
KUSTO_TENANT_ID = ""

# Local state directory - customize this path as needed
STATE_DIR = os.environ.get("STATE_DIR", "./state_data")

def get_kusto_client():
    """Create and return a KUSTO client."""
    try:
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            KUSTO_CLUSTER,
            KUSTO_CLIENT_ID,
            KUSTO_CLIENT_SECRET,
            KUSTO_TENANT_ID
        )
        client = KustoClient(kcsb)
        logger.info("Successfully created KUSTO client")
        return client
    except Exception as e:
        logger.error(f"Failed to create KUSTO client: {e}")
        raise

def get_kusto_ingest_client():
    """Create and return a KUSTO ingestion client."""
    try:
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            KUSTO_CLUSTER,
            KUSTO_CLIENT_ID,
            KUSTO_CLIENT_SECRET,
            KUSTO_TENANT_ID
        )
        client = KustoStreamingIngestClient(kcsb)
        logger.info("Successfully created KUSTO ingestion client")
        return client
    except Exception as e:
        logger.error(f"Failed to create KUSTO ingestion client: {e}")
        raise

def ensure_kusto_table_exists(client):
    """Ensure the Kusto table exists with the right schema and mapping."""
    try:
        # Check if table exists, create if it doesn't
        check_table_cmd = f".show table {KUSTO_TABLE} schema"
        client.execute_mgmt(KUSTO_DATABASE, check_table_cmd)
        logger.info(f"Table {KUSTO_TABLE} exists in database {KUSTO_DATABASE}")
    except Exception:
        # Create table with the required schema - without mae_1mins field
        create_table_cmd = f"""
        .create table {KUSTO_TABLE} (
            product_ids: dynamic,
            price: dynamic,
            quantity: dynamic,
            timestamp: datetime
        )
        """
        client.execute_mgmt(KUSTO_DATABASE, create_table_cmd)
        logger.info(f"Created table {KUSTO_TABLE} in database {KUSTO_DATABASE}")
    
    try:
        # Create mapping if it doesn't exist - fixed JSON format with proper escaping
        # Removed mae_1mins from mapping
        create_mapping_cmd = f"""
        .create table {KUSTO_TABLE} ingestion json mapping 'StockEventsMapping' '[
            {{"column":"product_ids","path":"$.product_ids","datatype":"dynamic"}},
            {{"column":"price","path":"$.price","datatype":"dynamic"}},
            {{"column":"quantity","path":"$.quantity","datatype":"dynamic"}},
            {{"column":"timestamp","path":"$.timestamp","datatype":"datetime"}}
        ]'
        """
        client.execute_mgmt(KUSTO_DATABASE, create_mapping_cmd)
        logger.info(f"Created mapping for table {KUSTO_TABLE}")
    except Exception as e:
        # Mapping might already exist
        logger.info(f"Mapping creation returned: {str(e)}")

def write_to_kusto(client, data):
    """Write data to KUSTO database using the queued ingestion client."""
    try:
        # Format data according to the expected Kusto schema
        # Removed mae_1mins field
        formatted_data = {
            "product_ids": data.get("product_ids", "unknown"),
            "price": float(data.get("price", 0.0)),
            "quantity": float(data.get("quantity", 0.0)),
            "timestamp": data.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        }
        
        
        # Convert formatted data dictionary to a JSON string
        json_data = json.dumps(formatted_data)
        
        # Instead of using a temporary file, let's use a direct string-based ingestion approach
        # Create ingestion properties
        ingestion_props = IngestionProperties(
            database=KUSTO_DATABASE,
            table=KUSTO_TABLE,
            data_format=DataFormat.JSON,
            
        )
        #ingestion_props.ingestion_mapping_reference = "JsonMapping"
        # Create a stream from the JSON string
        stream = io.StringIO(json_data)
        
        # Ingest the data directly from the stream
        result = client.ingest_from_stream(stream, ingestion_properties=ingestion_props)
        logger.info(f"Successfully queued data for ingestion to KUSTO: {formatted_data}")
        print(result.status)
        
    except Exception as e:
        logger.error(f"Error writing to KUSTO: {e}")
        import traceback
        logger.error(traceback.format_exc())

def parse_connection_string(connection_string):
    """Parse the Azure Event Hub connection string to extract components."""
    parts = {}
    for part in connection_string.split(';'):
        if '=' in part:
            key, value = part.split('=', 1)
            parts[key] = value
    return parts

def main():
    # Connection string details
    connection_string = ""
    event_hub_name = ""
    consumer_group_name = ""
    
    try:
        # Parse connection string to extract details
        conn_parts = parse_connection_string(connection_string)
        entity_path = conn_parts.get('EntityPath', event_hub_name)
        
        # Extract the namespace from the Endpoint
        if 'Endpoint' in conn_parts:
            endpoint = conn_parts['Endpoint']
            # Extract the namespace part (removing sb:// and rest)
            namespace = endpoint.replace('sb://', '').split('.')[0]
            bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
        else:
            raise ValueError("Connection string doesn't contain Endpoint")
        
        # Create ConnectionConfig with the required fields
        connection = ConnectionConfig(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="$ConnectionString",
            sasl_password=connection_string
        )
        
        # Extra Kafka configuration for compatibility with Azure Event Hubs
        kafka_extra_config = {
            "api.version.request": True,
            "broker.version.fallback": "1.0.0",
            "request.timeout.ms": 60000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000
        }
        
        # Ensure state directory exists
        os.makedirs(STATE_DIR, exist_ok=True)
        
        # Create KUSTO clients
        kusto_client = get_kusto_client()
        kusto_ingest_client = get_kusto_ingest_client()
        
        # Ensure the Kusto table exists with the right schema and mapping
        ensure_kusto_table_exists(kusto_client)
        
        # Initialize Quixstreams application with proper configuration
        app = Application(
            broker_address=connection,
            consumer_group=consumer_group_name,  # Use the provided consumer group
            auto_offset_reset="earliest",
            use_changelog_topics=False,  # Disable changelog topics
            state_dir=STATE_DIR,         # Specify state directory
            producer_extra_config=kafka_extra_config,
            consumer_extra_config=kafka_extra_config
        )
        
        # Create a topic with the entity path from connection string
        input_topic = app.topic(
            entity_path,
            value_deserializer='json'
        )
        
        # Create a StreamingDataFrame from the topic
        sdf = app.dataframe(input_topic)
        
        # Process incoming messages and extract required fields
        def process_message(value):
            logger.info(f"Received message: {value}")
            
            # Ensure the message has all required fields
            if 'product_id' not in value:
                value['product_id'] = 'unknown'
            
            if 'price' not in value:
                value['price'] = 0.0
            else:
                value['price'] = float(value['price'])
                
            if 'quantity' not in value:
                value['quantity'] = 0.0
            else:
                value['quantity'] = float(value['quantity'])
                
            if 'timestamp' not in value:
                value['timestamp'] = datetime.now().isoformat()
            
            # Write the message directly to Kusto
            write_to_kusto(kusto_ingest_client, value)
            
            return value
            
        # Process incoming data
        sdf = sdf.update(process_message)
        
        # Start the Quixstreams application
        logger.info(f"Starting Quixstreams application to process from topic {entity_path}")
        app.run()
    
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        logger.info("Application shutting down")

if __name__ == "__main__":
    main()