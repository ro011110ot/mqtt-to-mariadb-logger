import paho.mqtt.client as mqtt
import mysql.connector
from dotenv import dotenv_values
import ssl
import time

# --- Configuration Loading ---
CONFIG = dotenv_values(".config")

# ... (connect_db und setup_database bleiben gleich) ...

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
        print(f"Error connecting to MariaDB in callback: {err}")
        return None

def setup_database(db_conn):
    """Creates the necessary table if it does not exist."""
    cursor = db_conn.cursor()
    table_name = CONFIG["DB_TABLE"]
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        topic VARCHAR(255) NOT NULL,
        message TEXT,
        qos INT
    );
    """
    try:
        cursor.execute(create_table_query)
        db_conn.commit()
        print(f"Database table '{table_name}' is ready.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")
    finally:
        cursor.close()

# --- MQTT Callbacks (KRITISCH: Signaturen aktualisiert) ---

# NEUE SIGNATUR für V2: 5 Argumente
def on_connect(client, userdata, flags, reason_code, properties): 
    """Callback function when the client connects to the broker."""
    # Verwende reason_code.rc (oder rc, wenn er als Objekt übergeben wird)
    # Paho V2 API übergibt reason_code als Objekt, wir prüfen auf 0
    if reason_code == 0:
        print("Connected to MQTT Broker successfully.")
        topic = CONFIG["MQTT_TOPIC_SUBSCRIPTION"]
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect, return code {reason_code}") # Zeige den Fehlercode
        # Bei V2 ist reason_code ein Objekt, das in Strings aufgelöst werden kann

# NEUE SIGNATUR für V2: 4 Argumente (Nachricht hat sich geändert)
def on_message(client, userdata, msg):
    """
    Callback function when a message is received from the broker.
    (Keine Signaturänderung für on_message in V2, aber die anderen müssen angepasst werden)
    """
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    qos = msg.qos
    
    print(f"[{topic}] Received: {payload}")
    
    db_conn = connect_db() 
    
    if db_conn is None:
        print(" -> Log skipped: Could not establish a database connection.")
        return
        
    cursor = None
    try:
        cursor = db_conn.cursor()
        table_name = CONFIG["DB_TABLE"]
        
        add_message = f"""
        INSERT INTO {table_name} 
            (timestamp, topic, message, qos) 
        VALUES 
            (NOW(), %s, %s, %s)
        """
        
        data = (topic, payload, qos)
        
        cursor.execute(add_message, data)
        db_conn.commit()
        print(" -> Logged to MariaDB.")
    except mysql.connector.Error as err:
        print(f"Error logging to MariaDB: {err}")
    finally:
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()

# --- Main Logic ---

if __name__ == "__main__":
    
    # 1. Database Setup
    db_connection = connect_db()
    if db_connection is None:
         print("Initial database connection failed. Cannot proceed with setup.")
         exit(1)
         
    setup_database(db_connection)
    db_connection.close()
    
    # 2. MQTT Client Setup
    # Hier verwenden wir bewusst VERSION2, daher die Anpassung der Callbacks
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) 
    client.on_connect = on_connect
    client.on_message = on_message
    
    # 3. Handle SSL/TLS
    if CONFIG.get("MQTT_USE_SSL", "false").lower() == "true":
        print("Attempting connection with SSL/TLS.")
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
        
    # 4. Handle Credentials
    mqtt_user = CONFIG.get("MQTT_USER")
    mqtt_pass = CONFIG.get("MQTT_PASSWORD")
    if mqtt_user and mqtt_pass:
        client.username_pw_set(mqtt_user, mqtt_pass)
    
    # 5. Connection
    mqtt_host = CONFIG["MQTT_BROKER_HOST"]
    mqtt_port = int(CONFIG["MQTT_BROKER_PORT"])
    
    print(f"Connecting to MQTT broker at {mqtt_host}:{mqtt_port}...")
    try:
        client.connect(mqtt_host, mqtt_port, 60)
    except Exception as e:
        print(f"Could not connect to MQTT Broker: {e}")
        exit(1)
        
    # 6. Start the Loop
    print("Starting MQTT listener loop...")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred in the main loop: {e}")
        
    print("Program finished.")

