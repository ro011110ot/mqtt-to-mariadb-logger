import paho.mqtt.client as mqtt
import mysql.connector
from dotenv import dotenv_values
import ssl

# --- Configuration Loading ---
# Load configuration from the .config file.
# Note: The .config file is in .gitignore to protect credentials.
CONFIG = dotenv_values(".config")

# --- Database Connection ---
def connect_db():
    """Establishes and returns a connection to the MariaDB/MySQL database."""
    try:
        db = mysql.connector.connect(
            host=CONFIG["DB_HOST"],
            port=CONFIG["DB_PORT"],
            user=CONFIG["DB_USER"],
            password=CONFIG["DB_PASSWORD"],
            database=CONFIG["DB_NAME"]
        )
        return db
    except mysql.connector.Error as err:
        print(f"Error connecting to MariaDB: {err}")
        exit(1)

# --- Database Initialization ---
def setup_database(db_conn):
    """Creates the necessary table if it does not exist."""
    cursor = db_conn.cursor()
    table_name = CONFIG["DB_TABLE"]
    
    # Simple SQL command to create a table for logging messages
    # timestamp is stored as DATETIME, topic and message as TEXT
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

# --- MQTT Callbacks ---

def on_connect(client, userdata, flags, rc):
    """Callback function when the client connects to the broker."""
    if rc == 0:
        print("Connected to MQTT Broker successfully.")
        # Subscribe to the configured topic
        topic = CONFIG["MQTT_TOPIC_SUBSCRIPTION"]
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    """Callback function when a message is received from the broker."""
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    qos = msg.qos
    
    print(f"[{topic}] Received: {payload}")
    
    db_conn = userdata["db_connection"]
    table_name = CONFIG["DB_TABLE"]
    
    try:
        cursor = db_conn.cursor()
        
        # SQL to insert the message details
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
        if 'cursor' in locals() and cursor:
            cursor.close()

# --- Main Logic ---

if __name__ == "__main__":
    
    # 1. Database Setup
    db_connection = connect_db()
    setup_database(db_connection)
    
    # 2. MQTT Client Setup
    client = mqtt.Client(userdata={"db_connection": db_connection}) # Pass DB connection in userdata
    client.on_connect = on_connect
    client.on_message = on_message
    
    # 3. Handle SSL/TLS
    if CONFIG.get("MQTT_USE_SSL", "false").lower() == "true":
        print("Attempting connection with SSL/TLS.")
        client.tls_set(tls_version=ssl.PROTOCOL_TLS)
        
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
    # Blocks the program, processes incoming network traffic, dispatches callbacks
    client.loop_forever()
    
    # Close DB connection if loop_forever somehow terminates
    db_connection.close()
    print("Program terminated. Database connection closed.")

