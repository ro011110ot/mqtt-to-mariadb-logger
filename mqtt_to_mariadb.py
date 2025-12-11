import paho.mqtt.client as mqtt
import mysql.connector
from dotenv import dotenv_values
import ssl
import json
import time

# --- Configuration Loading ---
# Stellt sicher, dass Sie eine .config Datei mit DB_HOST, DB_USER, DB_PASSWORD, 
# DB_NAME, MQTT_BROKER_HOST, MQTT_BROKER_PORT und MQTT_TOPIC_SUBSCRIPTION haben.
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
    Preserves case (Groß-/Kleinschreibung).
    """
    # Ersetzt Slashes durch Unterstriche
    return topic.replace('/', '_').replace('+', '')

def table_exists(cursor, table_name):
    """Checks if a given table exists in the current database."""
    try:
        # Führt eine SELECT-Anweisung aus, die fehlschlägt, wenn die Tabelle nicht existiert
        cursor.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1;")
        return True
    except mysql.connector.Error as err:
        # Fehlercode 1146 ist 'Table doesn't exist'
        if err.errno == 1146:
            return False
        print(f"Error checking table existence for '{table_name}': {err}")
        return False

def python_type_to_sql(value):
    """Translates Python data type to a suitable MariaDB data type."""
    if isinstance(value, (int, float)):
        # DECIMALS bieten Genauigkeit für Sensorwerte
        return "DECIMAL(10, 4)" 
    elif isinstance(value, str):
        # VARCHAR für Text-Status, Namen etc.
        return "VARCHAR(255)"
    else:
        # Fallback für Listen, Booleans etc., die als Text gespeichert werden
        return "TEXT"

def create_dynamic_table(cursor, table_name, data):
    """
    Creates a new table based on the keys and determined types from the JSON data.
    """
    dynamic_columns = []
    
    # 1. Sammle Spaltendefinitionen basierend auf den Daten
    for key, value in data.items():
        # Spaltennamen bereinigen
        safe_key = key.replace('.', '_').replace('-', '_')
        
        # Bestimme den passenden SQL-Typ
        sql_type = python_type_to_sql(value)
        
        # Füge die Definition hinzu (NULL erlaubt, da nicht jede Nachricht alle Keys haben muss)
        dynamic_columns.append(f"`{safe_key}` {sql_type} NULL")
        
    # 2. Die gesamte CREATE-Anweisung zusammenbauen
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
        # Abonnieren des Wildcard-Topics (z.B. 'Sensoren/#')
        client.subscribe(topic) 
        print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect, return code {reason_code}")

def on_message(client, userdata, msg):
    
    topic = msg.topic
    
    # 1. Payload dekodieren und JSON parsen (Pflicht-Check)
    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str) 
        
    except json.JSONDecodeError:
        # Meldung überspringen, wenn kein gültiges JSON
        print(f"[{topic}] Log skipped: Payload is not valid JSON.")
        return
    except Exception as e:
        print(f"[{topic}] Log skipped: Error processing payload. {e}")
        return
        
    table_name = topic_to_table_name(topic)
    
    # 2. 'id' extrahieren und aus 'data' entfernen
    # Annahme: Der Sensor sendet seine ID unter dem Key 'id'
    sensor_id = str(data.pop('id', 'UNKNOWN')) 
    
    db_conn = connect_db() 
    if db_conn is None:
        return
        
    cursor = None
    try:
        cursor = db_conn.cursor()
        
        # 3. Tabellen-Check und Erstellung (wenn nötig)
        if not table_exists(cursor, table_name):
            # Erstellt die Tabelle basierend auf den aktuellen Daten-Keys und -Typen
            create_dynamic_table(cursor, table_name, data)

        # 4. Daten-Einfügung (Dynamischer INSERT)
        
        # Basisspalten
        insert_columns = ['timestamp', 'sensor_id']
        insert_values = ['NOW()', '%s']
        insert_data = [sensor_id]
        
        # Dynamische Spalten und Werte hinzufügen
        for key, value in data.items():
            safe_key = key.replace('.', '_').replace('-', '_')
            insert_columns.append(f"`{safe_key}`")
            insert_values.append('%s')
            insert_data.append(value) # value wird direkt übergeben (wird von mysql.connector gehandhabt)
            
        # Den Query zusammensetzen
        columns_str = ", ".join(insert_columns)
        values_str = ", ".join(insert_values)
        
        # Wichtig: Backticks um Tabellennamen (für Groß-/Kleinschreibung)
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

