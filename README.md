# MQTT to MariaDB Logger 💾

A simple Python script using `paho-mqtt` to subscribe to a specified MQTT topic (default: all topics `#`) and log the received messages, including the topic and payload, into a MariaDB/MySQL database.

## Prerequisites

* Python 3.x
* An operational MariaDB or MySQL server.
* An operational MQTT Broker (with optional SSL/TLS support).

## Setup and Installation

### 1. Clone the repository

```bash
git clone [YOUR_REPO_URL]
cd mqtt-to-mariadb-logger
```

### 2. Configure Credentials

The project uses a `.config` file to manage all connection credentials, which is added to `.gitignore` for security.

1.  Copy the example file to your actual configuration file:
    ```bash
    cp .config_example .config
    ```
2.  **Edit the `.config` file** and replace all placeholder values with your actual MQTT broker and MariaDB/MySQL credentials.

### 3. Install Dependencies

It is highly recommended to use a virtual environment.

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate 

# Install required Python packages
pip install -r requirements.txt
```

### 4. Database Preparation

Ensure that the database specified in your `.config` file (`DB_NAME`) exists on your MariaDB server and the configured user (`DB_USER`) has permissions to create and insert data into tables. The script will automatically create the table specified by `DB_TABLE` if it doesn't exist.

## Usage

Once configured and dependencies are installed, you can run the logger script:

```bash
python mqtt_to_mariadb.py
```

The script will connect, set up the database table, subscribe to the topic, and start logging messages.

## Configuration Details

The `.config` file requires the following parameters:

| Parameter | Description |
| :--- | :--- |
| `MQTT_BROKER_HOST` | Address of your MQTT Broker. |
| `MQTT_BROKER_PORT` | Port of your MQTT Broker (e.g., `1883` or `8883` for SSL). |
| `MQTT_USE_SSL` | Set to `true` to enable TLS/SSL connection. |
| `MQTT_USER`, `MQTT_PASSWORD` | Credentials for MQTT authentication. |
| `MQTT_TOPIC_SUBSCRIPTION` | The topic to subscribe to (e.g., `sensor/+/data` or `#` for all). |
| `DB_HOST`, `DB_PORT` | Address and port of the database server. |
| `DB_NAME`, `DB_TABLE` | Name of the database and the table for logs. |
| `DB_USER`, `DB_PASSWORD` | Credentials for database authentication. |

