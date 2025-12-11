# MQTT to MariaDB Logger 💾

A simple Python script using `paho-mqtt` to subscribe to specified MQTT topics and log the received JSON messages into a MariaDB/MySQL database.

**⚙️ Core Feature:** The script implements **dynamic table creation** and **dynamic schema inference** based on the received MQTT Topic and the structure of the JSON payload.

## 🚀 Dynamic Schema and Core Functionality

The logger follows a strict "Topic-to-Table" approach and infers the column types from the values of the first received JSON message for a new topic.

* **Topic to Table:** A topic like `Sensors/DHT11` is automatically mapped to the table `Sensors_DHT11`. (Case sensitivity is preserved, matching Linux filesystem standards.)
* **Dynamic Columns:** JSON payload keys are used as column names. The data type is automatically determined (e.g., `DECIMAL` for numbers, `VARCHAR` for strings).
* **Expected Payload Format:** The payload must be a **valid JSON object** and is expected to contain a unique identifier for the sensor:

    {"id": "LivingRoom_1", "temperature": 22.5, "humidity": 55.0}

* **Result:** The resulting table `Sensors_DHT11` will contain the columns `id`, `timestamp`, `sensor_id`, `temperature` (`DECIMAL`), and `humidity` (`DECIMAL`).

## Prerequisites

* Python 3.x
* An operational MariaDB or MySQL server.
* An operational MQTT Broker (with optional SSL/TLS support).

## Setup and Installation

### 1. Clone the repository

    git clone https://github.com/ro011110ot/mqtt-to-mariadb-logger.git
    cd mqtt-to-mariadb-logger

### 2. Configure Credentials

The project uses a `.config` file to manage all connection credentials, which is added to `.gitignore` for security.

* Copy the example file to your actual configuration file:

    cp .config_example .config

* Edit the `.config` file and replace all placeholder values with your actual MQTT broker and MariaDB/MySQL credentials.

> **Note:** The old `DB_TABLE` parameter is **no longer used** as table names are derived from the MQTT topics.

### 3. Install Dependencies

It is highly recommended to use a virtual environment.

    # Create and activate virtual environment
    python3 -m venv .venv
    source .venv/bin/activate 

    # Install required Python packages
    pip install -r requirements.txt

### 4. Database Preparation

Ensure that the database specified in your `.config` file (`DB_NAME`) exists on your MariaDB server and the configured user (`DB_USER`) has **permissions to CREATE and INSERT** data into tables.

The script will automatically create topic-specific tables upon receiving the first valid JSON message.

### 5. Production Setup (systemd Service)

To run the logger permanently in the background, set it up as a `systemd` service.

#### 5.1 Create Service File

Create the service file under `/etc/systemd/system/`:

    sudo nano /etc/systemd/system/mqtt-logger.service

Add the following content (replace `ro011110ot` with your username and update the `WorkingDirectory` path if necessary):

    [Unit]
    Description=MQTT to MariaDB Logger Service
    After=network.target mariadb.service

    [Service]
    Type=simple
    User=ro011110ot
    WorkingDirectory=/home/ro011110ot/scripts/mqtt-to-mariadb-logger
    # Explicit execution via shell to correctly activate the venv and ensure log collection
    ExecStart=/bin/sh -c '. .venv/bin/activate && python mqtt_to_mariadb.py'
    Restart=always
    RestartSec=5
    StandardOutput=journal
    StandardError=journal

    [Install]
    WantedBy=multi-user.target

#### 5.2 Activate and Start Service

Run the following commands to load the configuration, enable the service, and start it:

    # Reload configuration
    sudo systemctl daemon-reload

    # Enable service (starts automatically after server reboot)
    sudo systemctl enable mqtt-logger.service

    # Start service
    sudo systemctl start mqtt-logger.service

    # Check status (should show 'active (running)')
    sudo systemctl status mqtt-logger.service

    # Follow logs live (stdout/stderr)
    # sudo journalctl -u mqtt-logger.service -f

## Usage

Once configured and dependencies are installed, you can run the logger script:

    python mqtt_to_mariadb.py

The script will connect, subscribe to the configured topic, and dynamically create tables as new topic types are encountered.

## Configuration Details

The `.config` file requires the following parameters:

| Parameter | Description |
|---|---|
| `MQTT_BROKER_HOST` | Address of your MQTT Broker. |
| `MQTT_BROKER_PORT` | Port of your MQTT Broker (e.g., 1883 or 8883 for SSL). |
| `MQTT_USE_SSL` | Set to `true` to enable TLS/SSL connection. |
| `MQTT_USER`, `MQTT_PASSWORD` | Credentials for MQTT authentication. |
| `MQTT_TOPIC_SUBSCRIPTION` | The topic to subscribe to (e.g., `Sensors/#` for all sensors or `#` for all topics). |
| `DB_HOST`, `DB_PORT` | Address and port of the database server. |
| `DB_NAME` | Name of the database where the tables will be created. |
| `DB_USER`, `DB_PASSWORD` | Credentials for database authentication. |
