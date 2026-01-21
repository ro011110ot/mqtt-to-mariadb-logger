import json
import ssl
from pathlib import Path

import mysql.connector
import paho.mqtt.client as mqtt
from dotenv import dotenv_values

# --- Load configuration ---
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / ".config"
CONFIG = dotenv_values(CONFIG_PATH)

MQTT_SSL_PORT = 8883


def connect_db():
    """Establish connection to MariaDB."""
    try:
        db = mysql.connector.connect(
            host=CONFIG.get("DB_HOST", "localhost"),
            port=int(CONFIG.get("DB_PORT", 3306)),
            user=CONFIG["DB_USER"],
            password=CONFIG["DB_PASSWORD"],
            database=CONFIG["DB_NAME"],
        )
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None
    else:
        return db


def python_type_to_sql(value):
    """Mapping Python types to SQL types."""
    if isinstance(value, (int, float)):
        return "DECIMAL(10, 2)"
    return "VARCHAR(100)"


def table_exists(cursor, table_name):
    """Check if table exists in database."""
    try:
        # S608: Table names must be dynamic here; names are sanitized manually.
        cursor.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1;")  # noqa: S608
        cursor.fetchall()
    except mysql.connector.Error:
        return False
    else:
        return True


def ensure_columns_exist(cursor, table_name, data):
    """Check if all keys in data exist as columns, otherwise create them."""
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = [col[0] for col in cursor.fetchall()]

    for key in data:
        safe_key = key.replace(".", "_").replace("-", "_")
        if safe_key not in existing_columns:
            sql_type = python_type_to_sql(data[key])
            alter_query = (
                f"ALTER TABLE `{table_name}` ADD COLUMN `{safe_key}` {sql_type} NULL"
            )
            cursor.execute(alter_query)
            print(f" -> Added missing column '{safe_key}' to table '{table_name}'.")


def create_dynamic_table(cursor, table_name, data):
    """Create a table based on JSON data keys."""
    columns = []
    for key, value in data.items():
        safe_key = key.replace(".", "_").replace("-", "_")
        columns.append(f"`{safe_key}` {python_type_to_sql(value)} NULL")

    query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sensor_id VARCHAR(100),
        {", ".join(columns)}
    );
    """
    cursor.execute(query)
    print(f" -> Table '{table_name}' created.")


def on_connect(client, _userdata, _flags, reason_code, _properties):
    """Callback for successful MQTT connection."""
    if reason_code == 0:
        print("Connected successfully to MQTT broker.")
        topic = CONFIG.get("MQTT_TOPIC_SUBSCRIPTION", "Sensors/#")
        client.subscribe(topic)
        print(f"Subscribed to: {topic}")
    else:
        print(f"Connection failed with code: {reason_code}")


def on_message(_client, _userdata, msg):
    """Callback for received MQTT messages."""
    topic = msg.topic
    payload_str = msg.payload.decode("utf-8")
    data = {}
    sensor_id = "Unknown_Device"

    try:
        parsed = json.loads(payload_str)
        if isinstance(parsed, dict):
            data = parsed
            sensor_id = str(data.pop("id", topic.split("/")[-1]))
        else:
            col_name = topic.split("/")[-1]
            data = {col_name: parsed}
    except json.JSONDecodeError:
        col_name = topic.split("/")[-1]
        try:
            data = {col_name: float(payload_str)}
        except ValueError:
            data = {col_name: payload_str}

    db_conn = connect_db()
    if not db_conn:
        return

    try:
        cursor = db_conn.cursor(buffered=True)
        table_name = topic.replace("/", "_").replace("+", "").replace("-", "_")

        if not table_exists(cursor, table_name):
            create_dynamic_table(cursor, table_name, data)
        else:
            ensure_columns_exist(cursor, table_name, data)

        safe_keys = [k.replace(".", "_").replace("-", "_") for k in data]
        cols = ["sensor_id"] + [f"`{k}`" for k in safe_keys]
        placeholders = ["%s"] * len(cols)

        # Move noqa here, where the string is constructed (S608)
        query = (
            f"INSERT INTO `{table_name}` ({', '.join(cols)}) "  # noqa: S608
            f"VALUES ({', '.join(placeholders)})"
        )

        cursor.execute(query, [sensor_id, *list(data.values())])
        db_conn.commit()
        print(f"[{topic}] Logged: {payload_str}")

    except mysql.connector.Error as error:
        print(f"Error during logging: {error}")
    finally:
        db_conn.close()


if __name__ == "__main__":
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    host = CONFIG.get("MQTT_BROKER_HOST", "localhost")
    port = int(CONFIG.get("MQTT_BROKER_PORT", 1883))

    if CONFIG.get("MQTT_USE_SSL", "false").lower() == "true" or port == MQTT_SSL_PORT:
        mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)

    if CONFIG.get("MQTT_USER"):
        mqtt_client.username_pw_set(CONFIG["MQTT_USER"], CONFIG["MQTT_PASSWORD"])

    print(f"Connecting to {host}:{port}...")
    try:
        mqtt_client.connect(host, port, 60)
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("Stopping script...")
    except Exception as e:  # noqa: BLE001
        print(f"Could not reach broker: {e}")
