import paho.mqtt.client as mqtt
import mysql.connector
from dotenv import dotenv_values
import ssl
import json
import time

"""
MQTT to MariaDB Logger
----------------------
This script subscribes to an MQTT topic, parses incoming JSON messages, 
and logs them into a MariaDB/MySQL database.

Key Features:
- Dynamic Table Creation: Tables are created automatically based on the MQTT topic.
- Dynamic Schema: Columns are created based on the keys in the JSON payload.
- Type Inference: Maps Python types (str, int, float) to SQL types (VARCHAR, DECIMAL).
"""

# --- Configuration Loading ---
# Ensures that a .config file exists with DB_HOST, DB_USER, DB_PASSWORD,
# DB_NAME, MQTT_BROKER_HOST, MQTT_BROKER_PORT, and MQTT_TOPIC_SUBSCRIPTION.
CONFIG = dotenv_values(".config")


# --- Database / Helper Functions ---

def connect_db():
    """Establishes and returns a connection to the MariaDB/MySQL database."""
    try:
        db = mysql.connector.connect(
            host=CONFIG.get("DB_HOST", "localhost"),
            port=CONFIG.get("DB_PORT", 3306),
            user=CONFIG["DB_USER"],
            password=CONFIG["DB_PASSWORD"],
            database=CONFIG["DB_NAME"]
        )
        return db
    except mysql.connector.Error as err:
        print(f"Error connecting to MariaDB: {err}")
        return None


def topic_to_table_name(topic):
    """
    Converts a topic string (e.g., 'Sensoren/DHT11') to a safe table name.
    Preserves case sensitivity.
    """
    # Replaces slashes with underscores and removes plus signs
    return topic.replace('/', '_').replace('+', '')


def table_exists(cursor, table_name):
    """
    Checks if a given table exists in the current database.

    Args:
        cursor: The database cursor.
        table_name (str): Name of the table to check.

    Returns:
        bool: True if table exists, False otherwise.
    """
    try:
        # Executes a SELECT statement to check for table existence
        cursor.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1;")

        # IMPORTANT: The result must be consumed (fetched),
        # otherwise it blocks the next INSERT command (Unread result found error)!
        cursor.fetchone()

        return True
    except mysql.connector.Error as err:
        # Error code 1146 means 'Table doesn't exist'
        if err.errno == 1146:
            return False
        print(f"Error checking table existence for '{table_name}': {err}")
        return False


def python_type_to_sql(value):
    """
    Translates Python data type to a suitable MariaDB data type.

    Args:
        value: The value to analyze.

    Returns:
        str: The corresponding SQL data type definition.
    """
    if isinstance(value, (int, float)):
        # DECIMALS provide better precision for sensor values than FLOAT
        return "DECIMAL(10, 4)"
    elif isinstance(value, str):
        # VARCHAR for text status, names, IDs, etc.
        return "VARCHAR(255)"
    else:
        # Fallback for lists, booleans, dicts, etc., which are saved as text
        return "TEXT"


def create_dynamic_table(cursor, table_name, data):
    """
    Creates a new table based on the keys and determined types from the JSON data.

    Args:
        cursor: The database cursor.
        table_name (str): The name of the table to create.
        data (dict): The JSON data dictionary to derive the schema from.
    """
    dynamic_columns = []

    # 1. Collect column definitions based on the data
    for key, value in data.items():
        # Sanitize column names (replace dots and dashes with underscores)
        safe_key = key.replace('.', '_').replace('-', '_')

        # Determine the appropriate SQL type based on the value's type
        sql_type = python_type_to_sql(value)

        # Add the definition (NULL allowed, as not every message might have all keys)
        dynamic_columns.append(f"`{safe_key}` {sql_type} NULL")

    # 2. Assemble the complete CREATE statement
    columns_definition = ", ".join(dynamic_columns)

    create_query = f"""
    CREATE TABLE `{table_name}` (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        sensor_id VARCHAR(100) NOT NULL,
        {columns_definition}
    );
    """

    try:
        cursor.execute(create_query)
        print(f" -> NEW TABLE created: '{table_name}'. Schema: {columns_definition}")
        return True
    except mysql.connector.Error as err:
        print(f" -> ERROR creating table '{table_name}': {err}")
        return False


# --- MQTT Callbacks ---

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback function when the client connects to the broker (Paho V2)."""
    if reason_code == 0:
        print("Connected to MQTT Broker successfully.")
        topic = CONFIG["MQTT_TOPIC_SUBSCRIPTION"]
        # Subscribe to the wildcard topic (e.g., 'Sensoren/#')
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect, return code {reason_code}")


def on_message(client, userdata, msg):
    """
    Callback function when a message is received.
    Parses JSON, checks/creates table, and inserts data.
    """
    topic = msg.topic

    # 1. Decode payload and parse JSON (Mandatory check)
    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)

    except json.JSONDecodeError:
        # Skip log if payload is not valid JSON
        print(f"[{topic}] Log skipped: Payload is not valid JSON.")
        return
    except Exception as e:
        print(f"[{topic}] Log skipped: Error processing payload. {e}")
        return

    table_name = topic_to_table_name(topic)

    # 2. Extract 'id' and remove it from 'data' to treat it separately
    # Assumption: The sensor sends its ID under the key 'id'
    sensor_id = str(data.pop('id', 'UNKNOWN'))

    db_conn = connect_db()
    if db_conn is None:
        return

    cursor = None
    try:
        cursor = db_conn.cursor()

        # 3. Table check and creation (if necessary)
        if not table_exists(cursor, table_name):
            # Creates the table based on the current data keys and types
            create_dynamic_table(cursor, table_name, data)

        # 4. Data Insertion (Dynamic INSERT)

        # Base columns that are always present
        insert_columns = ['timestamp', 'sensor_id']
        insert_values = ['NOW()', '%s']
        insert_data = [sensor_id]

        # Add dynamic columns and values from the JSON payload
        for key, value in data.items():
            safe_key = key.replace('.', '_').replace('-', '_')
            insert_columns.append(f"`{safe_key}`")
            insert_values.append('%s')
            insert_data.append(value)  # value is passed directly (handled by mysql.connector)

        # Assemble the query string
        columns_str = ", ".join(insert_columns)
        values_str = ", ".join(insert_values)

        # Important: Backticks around table names to safely handle case sensitivity and special chars
        add_message = f"""
        INSERT INTO `{table_name}` 
            ({columns_str}) 
        VALUES 
            ({values_str})
        """

        cursor.execute(add_message, tuple(insert_data))
        db_conn.commit()
        print(f"[{topic}] -> Logged successfully to '{table_name}'.")

    except mysql.connector.Error as err:
        print(f"[{topic}] Error logging to MariaDB: {err}")
    finally:
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()


# --- Main Logic ---

if __name__ == "__main__":

    # 1. MQTT Client Setup (Paho V2 API)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    # 2. Handle SSL/TLS
    if CONFIG.get("MQTT_USE_SSL", "false").lower() == "true":
        print("Attempting connection with SSL/TLS.")
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)

    # 3. Handle Credentials
    mqtt_user = CONFIG.get("MQTT_USER")
    mqtt_pass = CONFIG.get("MQTT_PASSWORD")
    if mqtt_user and mqtt_pass:
        client.username_pw_set(mqtt_user, mqtt_pass)

    # 4. Connection
    mqtt_host = CONFIG["MQTT_BROKER_HOST"]
    mqtt_port = int(CONFIG["MQTT_BROKER_PORT"])

    print(f"Connecting to MQTT broker at {mqtt_host}:{mqtt_port}...")
    try:
        client.connect(mqtt_host, mqtt_port, 60)
    except Exception as e:
        print(f"Could not connect to MQTT Broker: {e}")
        exit(1)

    # 5. Start the Loop
    print("Starting MQTT listener loop...")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred in the main loop: {e}")

    print("Program finished.")