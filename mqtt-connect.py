#!/usr/bin/env python3
"""
MQTT Connect for Meshtastic Version 0.8.7 by https://github.com/pdxlocations

Many thanks to and protos code from: https://github.com/arankwende/meshtastic-mqtt-client & https://github.com/joshpirihi/meshtastic-mqtt
Encryption/Decryption help from: https://github.com/dstewartgo

Powered by Meshtastic™ https://meshtastic.org/
"""



#### Imports
try:
    from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2
    from meshtastic import BROADCAST_NUM
except ImportError:
    from meshtastic import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2, BROADCAST_NUM

import random
import threading
import sqlite3
import time
import ssl
import string
import sys
import configparser
from datetime import datetime
from time import mktime
from typing import Optional
import base64
import json
import re
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import paho.mqtt.client as mqtt

from models import Node

#################################
### BBS Server Integration
#################################

# BBS User States
user_states = {}

# Global state tracking
global_states = {}  # Main system: 'mail', 'main_menu', or None
submenu_states = {}  # Submenu within system: 'boards', 'posts', 'inbox', 'sent', etc.

# Node-Topic tracking
node_topic_map = {}  # Track which topic each node was last seen on

# Mail notification throttling
last_notification_time = {}  # Track last notification time per node to prevent spam

def update_user_state(user_id, state):
    """Update user state for BBS operations."""
    user_states[user_id] = state
    if debug:
        print(f"Updated user {user_id} state to: {state}")

def get_user_state(user_id):
    """Get current user state for BBS operations."""
    state = user_states.get(user_id, None)
    if debug:
        print(f"User {user_id} state: {state}")
    return state

def update_global_state(user_id, system):
    """Update global system state for user."""
    global_states[user_id] = system
    if debug:
        print(f"Updated user {user_id} global system to: {system}")

def get_global_state(user_id):
    """Get current global system state for user."""
    system = global_states.get(user_id, None)
    if debug:
        print(f"User {user_id} global system: {system}")
    return system

def update_submenu_state(user_id, submenu):
    """Update submenu state for user."""
    submenu_states[user_id] = submenu
    if debug:
        print(f"Updated user {user_id} submenu to: {submenu}")

def get_submenu_state(user_id):
    """Get current submenu state for user."""
    submenu = submenu_states.get(user_id, None)
    if debug:
        print(f"User {user_id} submenu: {submenu}")
    return submenu

def clear_user_states(user_id):
    """Clear all states for a user."""
    user_states[user_id] = None
    global_states[user_id] = None
    submenu_states[user_id] = None
    if debug:
        print(f"Cleared all states for user {user_id}")

def update_node_topic(node_id, topic):
    """Update which topic a node was last seen on."""
    node_topic_map[node_id] = topic
    if debug:
        print(f"Updated node {node_id} last seen topic to: {topic}")
        
def get_node_topic_for_direct_message(destination_id):
    """Get the topic where a node was last seen, formatted for sending direct messages."""
    base_topic = node_topic_map.get(destination_id, None)
    
    if base_topic:
        # Extract root topic and reconstruct with destination node ID
        # base_topic format: "msh/US/VA/RVA/2/e/LongFast/!somenode"
        topic_parts = base_topic.split('/')
        if len(topic_parts) >= 6:
            root_topic_part = '/'.join(topic_parts[:5]) + '/'
            destination_hex = '!' + hex(destination_id)[2:]
            direct_topic = root_topic_part + channel + "/" + destination_hex
            if debug:
                print(f"Converted base topic {base_topic} to direct topic {direct_topic} for node {destination_id}")
            return direct_topic
    
    return None

def get_node_topic(node_id):
    """Get the topic where a node was last seen."""
    topic = node_topic_map.get(node_id, None)
    if debug:
        print(f"Node {node_id} last seen on topic: {topic}")
        if topic is None:
            print(f"Current node_topic_map contains {len(node_topic_map)} entries:")
            for tracked_node, tracked_topic in node_topic_map.items():
                print(f"  Node {tracked_node}: {tracked_topic}")
    return topic

def should_throttle_notification(node_id):
    """Check if we should throttle notifications to prevent spam."""
    current_time = time.time()
    last_time = last_notification_time.get(node_id, 0)
    
    if current_time - last_time < mail_notification_min_interval:
        if debug:
            print(f"Throttling notification to {node_id} - last sent {current_time - last_time:.1f}s ago (min interval: {mail_notification_min_interval}s)")
        return True
    
    # Update the last notification time
    last_notification_time[node_id] = current_time
    return False

#################################
### Configuration Loading
#################################

def load_config():
    """Load configuration from config.ini file."""
    global mqtt_broker, mqtt_port, mqtt_username, mqtt_password, root_topic, root_topics, channel, key
    global node_number, client_long_name, client_short_name, lat, lon, alt
    global bbs_enabled, bbs_nodes, debug, auto_reconnect, auto_reconnect_delay
    global mail_notification_delay, mail_notification_throttle_delay, mail_notification_min_interval
    global print_service_envelope, print_message_packet, print_text_message, print_node_info
    global print_telemetry, print_failed_encryption_packet, print_position_report, color_text
    global display_encrypted_emoji, display_dm_emoji, display_lookup_button, display_private_dms
    global record_locations, node_info_interval_minutes
    
    config = configparser.ConfigParser()
    
    try:
        config.read('config.ini')
        
        # MQTT Connection Settings
        mqtt_broker = config.get('DEFAULT', 'mqtt_broker', fallback='mqtt.meshtastic.org')
        mqtt_port = config.getint('DEFAULT', 'mqtt_port', fallback=1883)
        mqtt_username = config.get('DEFAULT', 'mqtt_username', fallback='meshdev')
        mqtt_password = config.get('DEFAULT', 'mqtt_password', fallback='large4cats')
        root_topic_config = config.get('DEFAULT', 'root_topic', fallback='msh/US/2/e/')
        
        # Parse comma-separated root topics
        root_topics = [topic.strip() for topic in root_topic_config.split(',') if topic.strip()]
        root_topic = root_topics[0] if root_topics else 'msh/US/2/e/'  # Use first topic as primary
        
        channel = config.get('DEFAULT', 'channel', fallback='LongFast')
        key = config.get('DEFAULT', 'key', fallback='AQ==')
        
        # Node Settings
        node_number = config.getint('DEFAULT', 'node_number', fallback=2882380807)
        client_long_name = config.get('DEFAULT', 'long_name', fallback='MQTTastic')
        client_short_name = config.get('DEFAULT', 'short_name', fallback='MCM')
        
        # Position Settings
        lat = config.get('DEFAULT', 'lat', fallback='')
        lon = config.get('DEFAULT', 'lon', fallback='')
        alt = config.get('DEFAULT', 'alt', fallback='')
        
        # BBS Settings
        bbs_enabled = config.getboolean('DEFAULT', 'bbs_enabled', fallback=True)
        bbs_nodes_str = config.get('DEFAULT', 'bbs_nodes', fallback='[]')
        try:
            bbs_nodes = json.loads(bbs_nodes_str)
        except json.JSONDecodeError:
            bbs_nodes = []
        
        # Debug Settings
        debug = config.getboolean('DEFAULT', 'debug', fallback=True)
        auto_reconnect = config.getboolean('DEFAULT', 'auto_reconnect', fallback=False)
        auto_reconnect_delay = config.getfloat('DEFAULT', 'auto_reconnect_delay', fallback=1.0)
        
        # Mail notification timing settings
        mail_notification_delay = config.getfloat('DEFAULT', 'mail_notification_delay', fallback=6.0)
        mail_notification_throttle_delay = config.getfloat('DEFAULT', 'mail_notification_throttle_delay', fallback=2.0)
        mail_notification_min_interval = config.getfloat('DEFAULT', 'mail_notification_min_interval', fallback=30.0)
        print_service_envelope = config.getboolean('DEFAULT', 'print_service_envelope', fallback=False)
        print_message_packet = config.getboolean('DEFAULT', 'print_message_packet', fallback=False)
        print_text_message = config.getboolean('DEFAULT', 'print_text_message', fallback=False)
        print_node_info = config.getboolean('DEFAULT', 'print_node_info', fallback=False)
        print_telemetry = config.getboolean('DEFAULT', 'print_telemetry', fallback=False)
        print_failed_encryption_packet = config.getboolean('DEFAULT', 'print_failed_encryption_packet', fallback=False)
        print_position_report = config.getboolean('DEFAULT', 'print_position_report', fallback=False)
        color_text = config.getboolean('DEFAULT', 'color_text', fallback=False)
        display_encrypted_emoji = config.getboolean('DEFAULT', 'display_encrypted_emoji', fallback=True)
        display_dm_emoji = config.getboolean('DEFAULT', 'display_dm_emoji', fallback=True)
        display_lookup_button = config.getboolean('DEFAULT', 'display_lookup_button', fallback=False)
        display_private_dms = config.getboolean('DEFAULT', 'display_private_dms', fallback=False)
        record_locations = config.getboolean('DEFAULT', 'record_locations', fallback=False)
        
        # Node Info Settings
        node_info_interval_minutes = config.getint('DEFAULT', 'node_info_interval_minutes', fallback=15)
        
        if debug:
            print("Configuration loaded from config.ini")
            
    except Exception as e:
        print(f"Error loading config.ini: {str(e)}")
        print("Using default configuration values")
        
        # Set defaults if config file fails to load
        mqtt_broker = "mqtt.meshtastic.org"
        mqtt_port = 1883
        mqtt_username = "meshdev"
        mqtt_password = "large4cats"
        root_topics = ["msh/US/2/e/"]
        root_topic = "msh/US/2/e/"
        channel = "LongFast"
        key = "AQ=="
        node_number = 2882380807
        client_long_name = "MQTTastic"
        client_short_name = "MCM"
        lat = ""
        lon = ""
        alt = ""
        bbs_enabled = True
        bbs_nodes = []
        debug = True
        auto_reconnect = False
        auto_reconnect_delay = 1.0
        print_service_envelope = False
        print_message_packet = False
        print_text_message = False
        print_node_info = False
        print_telemetry = False
        print_failed_encryption_packet = False
        print_position_report = False
        color_text = False
        display_encrypted_emoji = True
        display_dm_emoji = True
        display_lookup_button = False
        display_private_dms = False
        record_locations = False
        node_info_interval_minutes = 15


def save_config():
    """Save current configuration to config.ini file."""
    config = configparser.ConfigParser()
    
    config['DEFAULT'] = {
        # MQTT Connection Settings
        'mqtt_broker': mqtt_broker,
        'mqtt_port': str(mqtt_port),
        'mqtt_username': mqtt_username,
        'mqtt_password': mqtt_password,
        'root_topic': ','.join(root_topics),
        'channel': channel,
        'key': key,
        
        # Node Settings
        'node_number': str(node_number),
        'long_name': client_long_name,
        'short_name': client_short_name,
        
        # Position Settings
        'lat': lat,
        'lon': lon,
        'alt': alt,
        
        # BBS Settings
        'bbs_enabled': str(bbs_enabled).lower(),
        'bbs_nodes': json.dumps(bbs_nodes),
        
        # Debug Settings
        'debug': str(debug).lower(),
        'auto_reconnect': str(auto_reconnect).lower(),
        'auto_reconnect_delay': str(auto_reconnect_delay),
        'print_service_envelope': str(print_service_envelope).lower(),
        'print_message_packet': str(print_message_packet).lower(),
        'print_text_message': str(print_text_message).lower(),
        'print_node_info': str(print_node_info).lower(),
        'print_telemetry': str(print_telemetry).lower(),
        'print_failed_encryption_packet': str(print_failed_encryption_packet).lower(),
        'print_position_report': str(print_position_report).lower(),
        'color_text': str(color_text).lower(),
        'display_encrypted_emoji': str(display_encrypted_emoji).lower(),
        'display_dm_emoji': str(display_dm_emoji).lower(),
        'display_lookup_button': str(display_lookup_button).lower(),
        'display_private_dms': str(display_private_dms).lower(),
        'record_locations': str(record_locations).lower(),
        
        # Node Info Settings
        'node_info_interval_minutes': str(node_info_interval_minutes)
    }
    
    try:
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        if debug:
            print("Configuration saved to config.ini")
    except Exception as e:
        print(f"Error saving config.ini: {str(e)}")

#################################
### Program variables

default_key = "1PG7OiApB1nwvP+rz05pAQ==" # AKA AQ==
db_file_path = "mmc.db"
reserved_ids = [1,2,3,4,4294967295]

# Load configuration from config.ini
load_config()

# Additional variables that depend on config
max_msg_len = mesh_pb2.Constants.DATA_PAYLOAD_LEN
key_emoji = "\U0001F511"
encrypted_emoji = "\U0001F512"
dm_emoji = "\u2192"

client_hw_model = 255

#################################
### Program Base Functions


def is_valid_hex(test_value: str, minchars: Optional[int], maxchars: int) -> bool:
    """Check if the provided string is valid hex.  Note that minchars and maxchars count INDIVIDUAL HEX LETTERS, inclusive.  Setting either to None means you don't care about that one."""

    if test_value.startswith('!'):
        test_value = test_value[1:]		#Ignore a leading exclamation point
    valid_hex_return: bool = all(c in string.hexdigits for c in test_value)

    decimal_value = int(test_value, 16)
    if decimal_value in reserved_ids:
        return False
    
    if minchars is not None:
        valid_hex_return = valid_hex_return and (minchars <= len(test_value))
    if maxchars is not None:
        valid_hex_return = valid_hex_return and (len(test_value) <= maxchars)

    return valid_hex_return

def set_topic():
    """Set the MQTT topics for subscribing and publishing."""
    if debug:
        print("set_topic")
    global subscribe_topics, publish_topic, node_number, node_name, root_topics
    node_name = '!' + hex(node_number)[2:]
    
    # Create subscription topics for all root topics
    subscribe_topics = [topic + channel + "/#" for topic in root_topics]
    
    # Use the first root topic for publishing
    publish_topic = root_topics[0] + channel + "/" + node_name
    
    if debug:
        print(f"Subscribe topics: {subscribe_topics}")
        print(f"Publish topic: {publish_topic}")

def current_time() -> str:
    """Return the current time (as an integer number of seconds since the epoch) as a string."""

    current_time_str = str(int(time.time()))
    return current_time_str


def format_time(time_str: str) -> str:
    """Convert the time string (number of seconds since the epoch) back to a datetime object."""

    timestamp: int = int(time_str)
    time_dt: datetime = datetime.fromtimestamp(timestamp)

    # Get the current datetime for comparison
    now = datetime.now()

    # Check if the provided time is from today
    if time_dt.date() == now.date():
        # If it's today, format as "H:M am/pm"
        time_formatted = time_dt.strftime("%I:%M %p")
    else:
        # If it's not today, format as "DD/MM/YY H:M:S"
        time_formatted = time_dt.strftime("%d/%m/%y %H:%M:%S")

    return time_formatted


def xor_hash(data: bytes) -> int:
    """Return XOR hash of all bytes in the provided string."""

    result = 0
    for char in data:
        result ^= char
    return result


def generate_hash(name: str, key: str) -> int:
    """?"""

    replaced_key = key.replace('-', '+').replace('_', '/')
    key_bytes = base64.b64decode(replaced_key.encode('utf-8'))
    h_name = xor_hash(bytes(name, 'utf-8'))
    h_key = xor_hash(key_bytes)
    result: int = h_name ^ h_key
    return result


def get_name_by_id(name_type: str, user_id: str) -> str:
    """See if we have a (long or short, as specified by "name_type") name for the given user_id."""

    # Convert the user_id to hex and prepend '!'
    hex_user_id: str = '!%08x' % user_id

    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Fetch the name based on the hex user ID
            if name_type == "long":
                result = db_cursor.execute(f'SELECT long_name FROM {table_name} WHERE user_id=?', (hex_user_id,)).fetchone()
            if name_type == "short":
                result = db_cursor.execute(f'SELECT short_name FROM {table_name} WHERE user_id=?', (hex_user_id,)).fetchone()

            if result:
                if debug:
                    print("found user in db: " + str(hex_user_id))
                return result[0]
            # If we don't find a user id in the db, ask for an id
            else:
                if user_id != BROADCAST_NUM:
                    if debug:
                        print("didn't find user in db: " + str(hex_user_id))
                    send_node_info(user_id, want_response=True)  # DM unknown user a nodeinfo with want_response
                return f"Unknown User ({hex_user_id})"

    except sqlite3.Error as e:
        print(f"SQLite error in get_name_by_id: {e}")

    finally:
        db_connection.close()


def sanitize_string(input_str: str) -> str:
    """Check if the string starts with a letter (a-z, A-Z) or an underscore (_), and replace all non-alpha/numeric/underscore characters with underscores."""

    if not re.match(r'^[a-zA-Z_]', input_str):
        # If not, add "_"
        input_str = '_' + input_str

    # Replace special characters with underscores (for database tables)
    sanitized_str: str = re.sub(r'[^a-zA-Z0-9_]', '_', input_str)
    return sanitized_str




def on_message(client, userdata, msg):					# pylint: disable=unused-argument
#################################
# Handle Presets
    """Callback function that accepts a meshtastic message from mqtt."""

    # Extract the topic to track node locations
    message_topic = msg.topic
    
    # if debug:
    #     print("on_message")
    se = mqtt_pb2.ServiceEnvelope()
    is_encrypted: bool = False
    try:
        se.ParseFromString(msg.payload)
        if print_service_envelope:
            print ("")
            print ("Service Envelope:")
            print (se)
        mp = se.packet

    except Exception as e:
        # Reduce noise - only print ServiceEnvelope errors in debug mode
        if debug:
            print(f"*** ServiceEnvelope: {str(e)}")
        return

    if len(msg.payload) > max_msg_len:
        if debug:
            print('Message too long: ' + str(len(msg.payload)) + ' bytes long, skipping.')
        return

    if mp.HasField("encrypted") and not mp.HasField("decoded"):
        decode_encrypted(mp)
        is_encrypted=True
    
    if print_message_packet:
        print ("")
        print ("Message Packet:")
        print(mp)

    # Track which topic this node was seen on (do this early for all message types)
    from_node = getattr(mp, "from")
    update_node_topic(from_node, message_topic)
    
    if mp.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
        try:
            text_payload = mp.decoded.payload.decode("utf-8")
            process_message(mp, text_payload, is_encrypted)
            # print(f"{text_payload}")
        except Exception as e:
            print(f"*** TEXT_MESSAGE_APP: {str(e)}")

    elif mp.decoded.portnum == portnums_pb2.NODEINFO_APP:
        info = mesh_pb2.User()
        try:
            info.ParseFromString(mp.decoded.payload)
            maybe_store_nodeinfo_in_db(info)
            if print_node_info:
                print("")
                print("NodeInfo:")
                print(info)
        except Exception as e:
            print(f"*** NODEINFO_APP: {str(e)}")

    elif mp.decoded.portnum == portnums_pb2.POSITION_APP:
        pos = mesh_pb2.Position()
        try:
            pos.ParseFromString(mp.decoded.payload)
            if record_locations:
                maybe_store_position_in_db(getattr(mp, "from"), pos, getattr(mp, "rx_rssi"))
        except Exception as e:
            print(f"*** POSITION_APP: {str(e)}")

    elif mp.decoded.portnum == portnums_pb2.TELEMETRY_APP:
        env = telemetry_pb2.Telemetry()
        try:
            env.ParseFromString(mp.decoded.payload)
        except Exception as e:
            print(f"*** TELEMETRY_APP: {str(e)}")

        rssi = getattr(mp, "rx_rssi")

        # Device Metrics
        device_metrics_dict = {
            'Battery Level': env.device_metrics.battery_level,
            'Voltage': round(env.device_metrics.voltage, 2),
            'Channel Utilization': round(env.device_metrics.channel_utilization, 1),
            'Air Utilization': round(env.device_metrics.air_util_tx, 1)
        }
        if rssi:
           device_metrics_dict["RSSI"] = rssi

        # Environment Metrics
        environment_metrics_dict = {
            'Temp': round(env.environment_metrics.temperature, 2),
            'Humidity': round(env.environment_metrics.relative_humidity, 0),
            'Pressure': round(env.environment_metrics.barometric_pressure, 2),
            'Gas Resistance': round(env.environment_metrics.gas_resistance, 2)
        }
        if rssi:
           environment_metrics_dict["RSSI"] = rssi

        # Power Metrics
            # TODO
        # Air Quality Metrics
            # TODO

        if print_telemetry:

            device_metrics_string = "From: " + get_name_by_id("short", getattr(mp, "from")) + ", "
            environment_metrics_string = "From: " + get_name_by_id("short", getattr(mp, "from")) + ", "

            # Only use metrics that are non-zero
            has_device_metrics = True
            has_environment_metrics = True
            has_device_metrics = all(value != 0 for value in device_metrics_dict.values())
            has_environment_metrics = all(value != 0 for value in environment_metrics_dict.values())

            # Loop through the dictionary and append non-empty values to the string
            for label, value in device_metrics_dict.items():
                if value is not None:
                    device_metrics_string += f"{label}: {value}, "

            for label, value in environment_metrics_dict.items():
                if value is not None:
                    environment_metrics_string += f"{label}: {value}, "

            # Remove the trailing comma and space
            device_metrics_string = device_metrics_string.rstrip(", ")
            environment_metrics_string = environment_metrics_string.rstrip(", ")

            # Print or use the final string
            if has_device_metrics:
                print(device_metrics_string)
            if has_environment_metrics:
                print(environment_metrics_string)

    elif mp.decoded.portnum == portnums_pb2.TRACEROUTE_APP:
        if mp.decoded.payload:
            routeDiscovery = mesh_pb2.RouteDiscovery()
            routeDiscovery.ParseFromString(mp.decoded.payload)

            try:
                route_string = " > ".join(get_name_by_id("long", node) for node in routeDiscovery.route) if routeDiscovery.route else ""
                routeBack_string = " > ".join(get_name_by_id("long", node) for node in routeDiscovery.route_back) if routeDiscovery.route_back else ""

                to_node = get_name_by_id("long", getattr(mp, 'to'))
                from_node = get_name_by_id("long", getattr(mp, 'from'))

                # Build the message without redundant arrows
                routes = [to_node]

                if routeBack_string:
                    routes.append(route_string)

                routes.append(from_node)

                routes.append(to_node)

                final_route = " > ".join(routes)
                message = f"{format_time(current_time())} >>> Route: {final_route}"

                # Only display traceroutes originating from yourself
                if getattr(mp, 'to') == int(node_number):
                    update_console(message, tag="info")

            except AttributeError as e:
                print(f"Error accessing route: {e}")
            except Exception as ex:
                print(f"Unexpected error: {ex}")



def decode_encrypted(mp):
    """Decrypt a meshtastic message."""

    try:
        # Convert key to bytes
        key_bytes = base64.b64decode(key.encode('ascii'))

        nonce_packet_id = getattr(mp, "id").to_bytes(8, "little")
        nonce_from_node = getattr(mp, "from").to_bytes(8, "little")

        # Put both parts into a single byte array.
        nonce = nonce_packet_id + nonce_from_node

        cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

        data = mesh_pb2.Data()
        data.ParseFromString(decrypted_bytes)
        mp.decoded.CopyFrom(data)

    except Exception as e:
        if print_message_packet:
            print(f"failed to decrypt: \n{mp}")
        if debug:
            print(f"*** Decryption failed: {str(e)}")


def process_message(mp, text_payload, is_encrypted):
    """Process a single meshtastic text message."""

    if debug:
        print("process_message")
    if not message_exists(mp):
        from_node = getattr(mp, "from")
        to_node = getattr(mp, "to")

        # Ignore messages from our own node to prevent processing our own outgoing messages
        if from_node == node_number:
            if debug:
                print("Ignoring message from our own node")
            return

        # Needed for ACK
        message_id = getattr(mp, "id")
        want_ack: bool = getattr(mp, "want_ack")

        sender_short_name = get_name_by_id("short", from_node)
        receiver_short_name = get_name_by_id("short", to_node)
        display_str = ""
        private_dm = False
        should_send_auto_response = True  # Default: send menu for any message

        if debug:
            print(f"Message from {from_node} to {to_node}, my node number is {node_number}")
            print(f"Message content: {text_payload}")

        if to_node == node_number:
            if debug:
                print("This is a direct message to me!")
                print(f"Processing DM from {from_node}: '{text_payload}'")
                print(f"Current states - Global: {get_global_state(from_node)}, Submenu: {get_submenu_state(from_node)}, User: {get_user_state(from_node)}")
                if text_payload.strip().isdigit():
                    print(f"Text payload is digit: {text_payload.strip().isdigit()}")
                    print(f"Digit value: {int(text_payload.strip())}")
                    print(f"Digit >= 1: {int(text_payload.strip()) >= 1}")
                    print(f"Global state == 'mail': {get_global_state(from_node) == 'mail'}")
                    print(f"Submenu state check: {get_submenu_state(from_node) in ['inbox', 'sent', 'viewing_inbox']}")
            display_str = f"{format_time(current_time())} DM from {sender_short_name}: {text_payload}"
            if display_dm_emoji:
                display_str = display_str[:9] + dm_emoji + display_str[9:]
            if want_ack is True:
                send_ack(from_node, message_id)
            
            should_send_auto_response = True  # Default to sending auto-response
            
            # Check if this is a mail menu selection when in mail system and at mail menu
            if len(text_payload.strip()) == 1 and text_payload.strip().upper() in ['C', 'I', 'S', 'D'] and get_global_state(from_node) == 'mail' and get_submenu_state(from_node) == 'mail_menu':
                should_send_auto_response = False
                if debug:
                    print(f"Mail menu selection detected: {text_payload}")
                # Handle mail menu selection in a separate thread with a small delay
                def delayed_mail_response():
                    time.sleep(1.0)  # Wait 1 second
                    handle_mail_selection(from_node, text_payload)
                
                mail_thread = threading.Thread(target=delayed_mail_response, daemon=True)
                mail_thread.start()
            # Check if this is a compose command when viewing inbox
            elif text_payload.strip().upper() == 'C' and get_global_state(from_node) == 'mail' and (get_submenu_state(from_node) == 'inbox' or get_submenu_state(from_node) == 'viewing_inbox'):
                should_send_auto_response = False
                if debug:
                    print(f"Compose mail command detected from inbox view: {from_node}")
                # Handle compose mail command in a separate thread with a small delay
                def delayed_compose_from_inbox_response():
                    time.sleep(1.0)  # Wait 1 second
                    send_compose_mail_menu(from_node)
                
                compose_from_inbox_thread = threading.Thread(target=delayed_compose_from_inbox_response, daemon=True)
                compose_from_inbox_thread.start()
            # Check if this is a mail number selection when viewing inbox or sent mail in mail system
            elif text_payload.strip().isdigit() and int(text_payload.strip()) >= 1 and get_global_state(from_node) == 'mail' and (get_submenu_state(from_node) == 'inbox' or get_submenu_state(from_node) == 'sent' or get_submenu_state(from_node) == 'viewing_inbox'):
                should_send_auto_response = False
                mail_number = int(text_payload.strip())
                if debug:
                    print(f"Mail selection detected: {mail_number}")
                    print(f"Global state: {get_global_state(from_node)}")
                    print(f"Submenu state: {get_submenu_state(from_node)}")
                    print(f"User state: {get_user_state(from_node)}")
                # Handle mail selection in a separate thread with a small delay
                def delayed_mail_content_response():
                    time.sleep(1.0)  # Wait 1 second
                    submenu_state = get_submenu_state(from_node)
                    if submenu_state in ['inbox', 'viewing_inbox']:
                        send_mail_content(from_node, 'inbox', mail_number)
                    elif submenu_state == 'sent':
                        send_mail_content(from_node, 'sent', mail_number)
                
                mail_content_thread = threading.Thread(target=delayed_mail_content_response, daemon=True)
                mail_content_thread.start()
            # Check if this is a post number selection (1-3) when viewing posts in bulletin system
            elif text_payload.strip().isdigit() and 1 <= int(text_payload.strip()) <= 3 and get_global_state(from_node) == 'bulletin' and get_submenu_state(from_node) == 'posts':
                should_send_auto_response = False
                post_number = int(text_payload.strip())
                if debug:
                    print(f"Post selection detected: {post_number}")
                # Handle post selection in a separate thread with a small delay
                def delayed_post_response():
                    time.sleep(1.0)  # Wait 1 second
                    # Get the current board from user state
                    user_state = get_user_state(from_node)
                    if debug:
                        print(f"Post selection - User state: {user_state}")
                    if user_state and user_state.startswith('viewing_board:'):
                        board_name = user_state.split(':')[1]
                        if debug:
                            print(f"Post selection - Using board: {board_name}")
                        send_post_content(from_node, board_name, post_number)
                    else:
                        # Fallback to GENERAL if board not tracked
                        if debug:
                            print(f"Post selection - No board state, using GENERAL")
                        send_post_content(from_node, "GENERAL", post_number)
                
                post_thread = threading.Thread(target=delayed_post_response, daemon=True)
                post_thread.start()
            # Check if this is a board number selection (1-4) when in bulletin system
            elif text_payload.strip().isdigit() and 1 <= int(text_payload.strip()) <= 4 and get_global_state(from_node) == 'bulletin' and get_submenu_state(from_node) == 'boards':
                should_send_auto_response = False
                board_number = int(text_payload.strip())
                if debug:
                    print(f"Board selection detected: {board_number}")
                # Handle board selection in a separate thread with a small delay
                def delayed_board_response():
                    time.sleep(1.0)  # Wait 1 second
                    if debug:
                        print(f"Board selection - Sending posts for board {board_number}")
                    send_board_posts(from_node, board_number)
                
                board_thread = threading.Thread(target=delayed_board_response, daemon=True)
                board_thread.start()
            # Check if this is a back command when in mail system
            elif text_payload.strip().upper() == 'B' and get_global_state(from_node) == 'mail':
                should_send_auto_response = False
                if debug:
                    print(f"Mail back command detected from {from_node}")
                # Handle mail back command based on current submenu state
                def delayed_mail_back_response():
                    time.sleep(1.0)  # Wait 1 second
                    submenu_state = get_submenu_state(from_node)
                    if debug:
                        print(f"Mail back command - Current submenu: {submenu_state}")
                    
                    if submenu_state == 'viewing_mail':
                        # Going back from viewing mail to inbox/sent
                        user_state = get_user_state(from_node)
                        if user_state and user_state.startswith('viewing_mail:'):
                            parts = user_state.split(':')
                            mail_type = parts[1]
                            if debug:
                                print(f"Mail back command - Going back to {mail_type}")
                            if mail_type == 'inbox':
                                update_submenu_state(from_node, 'inbox')
                                send_inbox(from_node)
                            elif mail_type == 'sent':
                                update_submenu_state(from_node, 'sent')
                                send_sent_mail(from_node)
                    elif submenu_state in ['inbox', 'sent', 'delete_mail', 'compose_mail', 'delete_inbox', 'delete_sent']:
                        # Going back from inbox/sent/delete/compose to mail menu
                        if debug:
                            print(f"Mail back command - Going back to mail menu")
                        update_submenu_state(from_node, 'mail_menu')
                        send_mail_menu(from_node)
                    else:
                        # Going back from mail menu to main menu
                        if debug:
                            print(f"Mail back command - Going back to main menu")
                        clear_user_states(from_node)
                        # Send welcome menu
                        welcome_text = "✉️Welcome to Mesh Mail!✉️"
                        send_direct_message(from_node, welcome_text)
                
                mail_back_thread = threading.Thread(target=delayed_mail_back_response, daemon=True)
                mail_back_thread.start()
            # Check if this is content input for mail composition (highest priority - most specific state)
            elif get_global_state(from_node) == 'mail' and get_user_state(from_node) and get_user_state(from_node).startswith('composing_subject:'):
                should_send_auto_response = False
                if debug:
                    print(f"Mail content input detected: {text_payload}")
                # Handle content input in a separate thread with a small delay
                def delayed_content_response():
                    time.sleep(1.0)  # Wait 1 second
                    handle_compose_content(from_node, text_payload)
                
                content_thread = threading.Thread(target=delayed_content_response, daemon=True)
                content_thread.start()
            # Check if this is subject input for mail composition (medium priority)
            elif get_global_state(from_node) == 'mail' and get_user_state(from_node) and get_user_state(from_node).startswith('composing_to:'):
                should_send_auto_response = False
                if debug:
                    print(f"Mail subject input detected: {text_payload}")
                # Handle subject input in a separate thread with a small delay
                def delayed_subject_response():
                    time.sleep(1.0)  # Wait 1 second
                    handle_compose_subject(from_node, text_payload)
                
                subject_thread = threading.Thread(target=delayed_subject_response, daemon=True)
                subject_thread.start()
            # Check if this is recipient input for mail composition (lowest priority - most general state)
            elif get_global_state(from_node) == 'mail' and get_submenu_state(from_node) == 'compose_mail' and get_user_state(from_node) == 'composing_mail':
                should_send_auto_response = False
                if debug:
                    print(f"Mail recipient input detected: {text_payload}")
                    print(f"Global state: {get_global_state(from_node)}")
                    print(f"Submenu state: {get_submenu_state(from_node)}")
                # Handle mail composition in a separate thread with a small delay
                def delayed_compose_response():
                    time.sleep(1.0)  # Wait 1 second
                    handle_compose_mail(from_node, text_payload)
                
                compose_thread = threading.Thread(target=delayed_compose_response, daemon=True)
                compose_thread.start()
            # Check if this is a mail number selection when deleting from inbox or sent mail
            elif text_payload.strip().isdigit() and int(text_payload.strip()) >= 1 and get_global_state(from_node) == 'mail' and (get_submenu_state(from_node) == 'delete_inbox' or get_submenu_state(from_node) == 'delete_sent'):
                should_send_auto_response = False
                mail_number = int(text_payload.strip())
                if debug:
                    print(f"Delete mail selection detected: {mail_number}")
                # Handle mail deletion in a separate thread with a small delay
                def delayed_delete_mail_response():
                    time.sleep(1.0)  # Wait 1 second
                    submenu_state = get_submenu_state(from_node)
                    if submenu_state == 'delete_inbox':
                        delete_mail_by_number(from_node, 'inbox', mail_number)
                    elif submenu_state == 'delete_sent':
                        delete_mail_by_number(from_node, 'sent', mail_number)
                
                delete_mail_thread = threading.Thread(target=delayed_delete_mail_response, daemon=True)
                delete_mail_thread.start()
            # Check if this is a delete mail menu selection
            elif text_payload.strip().upper() in ['I', 'S', 'B'] and get_global_state(from_node) == 'mail' and get_submenu_state(from_node) == 'delete_mail':
                should_send_auto_response = False
                if debug:
                    print(f"Delete mail menu selection detected: {text_payload}")
                # Handle delete mail menu selection in a separate thread with a small delay
                def delayed_delete_menu_response():
                    time.sleep(1.0)  # Wait 1 second
                    handle_delete_mail_selection(from_node, text_payload)
                
                delete_menu_thread = threading.Thread(target=delayed_delete_menu_response, daemon=True)
                delete_menu_thread.start()
            # Check if this is a delete command when viewing a mail
            elif text_payload.strip().upper() == 'D' and get_global_state(from_node) == 'mail' and get_submenu_state(from_node) == 'viewing_mail':
                should_send_auto_response = False
                if debug:
                    print(f"Delete mail command detected from {from_node}")
                # Handle delete mail command
                def delayed_delete_mail_response():
                    time.sleep(1.0)  # Wait 1 second
                    user_state = get_user_state(from_node)
                    if user_state and user_state.startswith('viewing_mail:'):
                        parts = user_state.split(':')
                        mail_type = parts[1]
                        mail_id = int(parts[2])  # Get the specific mail ID
                        
                        # Delete the specific mail
                        delete_mail(mail_id)
                        
                        # Send confirmation message
                        success_msg = "Message Deleted"
                        time.sleep(2.0)
                        send_direct_message(from_node, success_msg)
                        
                        # Wait a moment then show inbox again
                        time.sleep(2.0)
                        if mail_type == 'inbox':
                            update_submenu_state(from_node, 'inbox')
                            send_inbox(from_node, show_instructions=True)
                        else:  # sent
                            update_submenu_state(from_node, 'sent')
                            send_sent_mail(from_node)
                    else:
                        error_msg = "Error: No mail context found."
                        send_direct_message(from_node, error_msg)
                
                delete_mail_thread = threading.Thread(target=delayed_delete_mail_response, daemon=True)
                delete_mail_thread.start()
            # Check if this is a reply command when viewing a mail
            elif text_payload.strip().upper() == 'R' and get_global_state(from_node) == 'mail' and get_submenu_state(from_node) == 'viewing_mail':
                should_send_auto_response = False
                if debug:
                    print(f"Reply mail command detected from {from_node}")
                # Handle reply mail command
                def delayed_reply_mail_response():
                    time.sleep(1.0)  # Wait 1 second
                    user_state = get_user_state(from_node)
                    if user_state and user_state.startswith('viewing_mail:'):
                        parts = user_state.split(':')
                        mail_type = parts[1]
                        mail_id = int(parts[2])  # Get the specific mail ID
                        
                        # Get the mail content to find the original sender
                        mail_data = get_mail_content(mail_id)
                        if mail_data and mail_type == 'inbox':
                            # For inbox mail, reply to the sender
                            original_sender = mail_data['from_node_id']
                            original_subject = mail_data['subject']
                            
                            # Set up reply composition with pre-filled subject
                            reply_subject = f"Re: {original_subject}" if not original_subject.startswith("Re: ") else original_subject
                            update_user_state(from_node, f'composing_subject:{original_sender}:{reply_subject}')
                            update_submenu_state(from_node, 'compose_mail')
                            
                            # Ask for content directly (skip subject since it's pre-filled)
                            content_prompt = f"Reply to: {original_sender}\nSubject: {reply_subject}\n\nEnter your reply message:"
                            time.sleep(2.0)
                            send_direct_message(from_node, content_prompt)
                        else:
                            error_msg = "Cannot reply to sent mail or mail not found."
                            send_direct_message(from_node, error_msg)
                    else:
                        error_msg = "Error: No mail context found for reply."
                        send_direct_message(from_node, error_msg)
                
                reply_mail_thread = threading.Thread(target=delayed_reply_mail_response, daemon=True)
                reply_mail_thread.start()
            else:
                # Mark that we should send welcome menu after processing
                should_send_auto_response = True
                if debug:
                    print(f"Marked for welcome menu to {from_node}")
                    print(f"Global state: {get_global_state(from_node)}")
                    print(f"Submenu state: {get_submenu_state(from_node)}")
                    print(f"User state: {get_user_state(from_node)}")
                    print(f"Text payload: '{text_payload}'")

            # Send auto-response after message is fully processed (only for direct messages)
            if should_send_auto_response:
                if debug:
                    print(f"Message processing complete, now sending welcome menu to {from_node}")
                # Send welcome menu in a separate thread with a small delay
                def delayed_auto_response():
                    time.sleep(3.0)  # Wait 3 seconds
                    # Use the exact same function that works for test direct messages
                    send_auto_response_via_test_function(from_node)
                
                response_thread = threading.Thread(target=delayed_auto_response, daemon=True)
                response_thread.start()

        elif from_node == node_number and to_node != BROADCAST_NUM:
            display_str = f"{format_time(current_time())} DM to {receiver_short_name}: {text_payload}"

        elif from_node != node_number and to_node != BROADCAST_NUM:
            if display_private_dms:
                display_str = f"{format_time(current_time())} DM from {sender_short_name} to {receiver_short_name}: {text_payload}"
                if display_dm_emoji:
                    display_str = display_str[:9] + dm_emoji + display_str[9:]
            else:
                if debug:
                    print("Private DM Ignored")
                private_dm = True

        else:
            display_str = f"{format_time(current_time())} {sender_short_name}: {text_payload}"

        if is_encrypted and not private_dm:
            color="encrypted"
            if display_encrypted_emoji:
                display_str = display_str[:9] + encrypted_emoji + display_str[9:]
        else:
            color="unencrypted"
        if not private_dm:
            update_console(display_str, tag=color)
        m_id = getattr(mp, "id")
        insert_message_to_db(current_time(), sender_short_name, text_payload, m_id, is_encrypted)

        text = {
            "message": text_payload,
            "from": getattr(mp, "from"),
            "id": getattr(mp, "id"),
            "to": getattr(mp, "to")
        }
        rssi = getattr(mp, "rx_rssi")
        if rssi:
            text["RSSI"] = rssi
        if print_text_message:
            print("")
            print(text)
    else:
        if debug:
            print("duplicate message ignored")


def message_exists(mp) -> bool:
    """Check for message id in db, ignore duplicates."""

    if debug:
        print("message_exists")
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Check if a record with the same message_id already exists
            existing_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE message_id=?', (str(getattr(mp, "id")),)).fetchone()

            return existing_record is not None

    except sqlite3.Error as e:
        print(f"SQLite error in message_exists: {e}")

    finally:
        db_connection.close()


#################################
# Send Messages

def direct_message(destination_id):
    """Send a direct message."""

    if debug:
        print("direct_message")
    if destination_id:
        try:
            destination_id = int(destination_id[1:], 16)
            publish_message(destination_id)
        except Exception as e:
            if debug:
                print(f"Error converting destination_id: {e}")

def publish_message(destination_id):
    """?"""

    if debug:
        print("publish_message")

    if not client.is_connected():
        connect_mqtt()

    print("publish_message: GUI removed - function disabled")


def send_traceroute(destination_id):
    """Send traceroute request to destination_id."""

    if debug:
        print("send_TraceRoute")

    if not client.is_connected():
        message =  format_time(current_time()) + " >>> Connect to a broker before sending traceroute"
        update_console(message, tag="info")
    else:
        message =  format_time(current_time()) + " >>> Sending Traceroute Packet"
        update_console(message, tag="info")

        if debug:
            print(f"Sending Traceroute Packet to {str(destination_id)}")

        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TRACEROUTE_APP
        encoded_message.want_response = True
        encoded_message.bitfield = 1

        destination_id = int(destination_id[1:], 16)
        generate_mesh_packet(destination_id, encoded_message)

def send_node_info(destination_id, want_response):
    """Send my node information to the specified destination."""

    global node_number

    if debug:
        print("send_node_info")

    if not client.is_connected():
        message =  format_time(current_time()) + " >>> Connect to a broker before sending nodeinfo"
        update_console(message, tag="info")
    else:
        if not move_text_up(): # copy ID to Number and test for 8 bit hex
            return
        
        if destination_id == BROADCAST_NUM:
            message =  format_time(current_time()) + " >>> Broadcast NodeInfo Packet"
            update_console(message, tag="info")
        else:
            if debug:
                print(f"Sending NodeInfo Packet to {str(destination_id)}")

        # Use config values instead of GUI entries
        decoded_client_id = bytes(node_name, "utf-8")
        decoded_client_long = bytes(client_long_name, "utf-8")
        decoded_client_short = bytes(client_short_name, "utf-8")
        decoded_client_hw_model = client_hw_model

        user_payload = mesh_pb2.User()
        setattr(user_payload, "id", decoded_client_id)
        setattr(user_payload, "long_name", decoded_client_long)
        setattr(user_payload, "short_name", decoded_client_short)
        setattr(user_payload, "hw_model", decoded_client_hw_model)

        user_payload = user_payload.SerializeToString()

        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.NODEINFO_APP
        encoded_message.payload = user_payload
        encoded_message.want_response = want_response  # Request NodeInfo back
        encoded_message.bitfield = 1

        # print(encoded_message)
        generate_mesh_packet(destination_id, encoded_message)


def send_position(destination_id) -> None:
    """Send current position to destination_id (which can be a broadcast.)"""

    global node_number

    if debug:
        print("send_Position")

    if not client.is_connected():
        message =  format_time(current_time()) + " >>> Connect to a broker before sending position"
        update_console(message, tag="info")
    else:
        if destination_id == BROADCAST_NUM:
            message =  format_time(current_time()) + " >>> Broadcast Position Packet"
            update_console(message, tag="info")
        else:
            if debug:
                print(f"Sending Position Packet to {str(destination_id)}")

        # Use config values instead of GUI entries
        latitude_str = lat
        longitude_str = lon

        try:
            latitude = float(latitude_str)  # Convert latitude to a float
        except ValueError:
            latitude = 0.0
        try:
            longitude = float(longitude_str)  # Convert longitude to a float
        except ValueError:
            longitude = 0.0

        latitude = latitude * 1e7
        longitude = longitude * 1e7

        latitude_i = int(latitude)
        longitude_i = int(longitude)

        # Use config value instead of GUI entry
        altitude_str = alt
        altitude_units = 1 / 3.28084 if 'ft' in altitude_str else 1.0
        altitude_number_of_units = float(re.sub('[^0-9.]','', altitude_str)) if altitude_str else 0.0
        altitude_i = int(altitude_units * altitude_number_of_units) # meters
        
        pos_time = int(time.time())

        position_payload = mesh_pb2.Position()
        setattr(position_payload, "latitude_i", latitude_i)
        setattr(position_payload, "longitude_i", longitude_i)
        setattr(position_payload, "altitude", altitude_i)
        setattr(position_payload, "time", pos_time)

        position_payload = position_payload.SerializeToString()

        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.POSITION_APP
        encoded_message.payload = position_payload
        encoded_message.want_response = True
        encoded_message.bitfield = 1

        generate_mesh_packet(destination_id, encoded_message)



def generate_mesh_packet(destination_id, encoded_message):
    """Send a packet out over the mesh."""

    global global_message_id
    mesh_packet = mesh_pb2.MeshPacket()

    # Use the global message ID and increment it for the next call
    mesh_packet.id = global_message_id
    global_message_id += 1

    setattr(mesh_packet, "from", node_number)
    mesh_packet.to = destination_id
    mesh_packet.want_ack = True  # Request acknowledgment for all messages
    mesh_packet.channel = generate_hash(channel, key)
    
    # Adjust hop settings for better delivery
    if destination_id != BROADCAST_NUM:
        # For direct messages, use more hops to ensure delivery
        mesh_packet.hop_limit = 3
        mesh_packet.hop_start = 3
    else:
        # For broadcast messages, use standard settings
        mesh_packet.hop_limit = 3
        mesh_packet.hop_start = 3

    if debug:
        print(f"Generating mesh packet: from={node_number}, to={destination_id}, id={mesh_packet.id}, hops={mesh_packet.hop_limit}")

    if key == "":
        mesh_packet.decoded.CopyFrom(encoded_message)
        if debug:
            print("key is none")
    else:
        mesh_packet.encrypted = encrypt_message(channel, key, mesh_packet, encoded_message)
        if debug:
            print("key present")

    service_envelope = mqtt_pb2.ServiceEnvelope()
    service_envelope.packet.CopyFrom(mesh_packet)
    service_envelope.channel_id = channel
    service_envelope.gateway_id = node_name
    # print (service_envelope)

    payload = service_envelope.SerializeToString()
    set_topic()
    
    # For broadcast messages (like node info), publish to ALL topics
    if destination_id == BROADCAST_NUM:
        if debug:
            print(f"Broadcasting to all topics")
            print(f"Payload size: {len(payload)} bytes")
        
        # Publish to all root topics
        for i, root_topic in enumerate(root_topics):
            broadcast_topic = root_topic + channel + "/" + node_name
            if debug:
                print(f"Publishing to topic {i+1}/{len(root_topics)}: {broadcast_topic}")
            
            result = client.publish(broadcast_topic, payload)
            
            if debug:
                print(f"MQTT publish result for {broadcast_topic}: {result.rc}")
                if result.rc == 0:
                    print(f"MQTT publish successful to {broadcast_topic}")
                else:
                    print(f"MQTT publish failed to {broadcast_topic} with code: {result.rc}")
    else:
        # For direct messages, try to send to recipient's last known region with their node ID
        recipient_topic = get_node_topic_for_direct_message(destination_id)
        
        if recipient_topic:
            # Send to the specific topic where recipient was last seen
            if debug:
                print(f"Sending direct message to recipient's targeted topic: {recipient_topic}")
                print(f"Payload size: {len(payload)} bytes")
            
            result = client.publish(recipient_topic, payload)
            
            if debug:
                print(f"MQTT publish result for {recipient_topic}: {result.rc}")
                if result.rc == 0:
                    print(f"Direct message published successfully to {recipient_topic}")
                else:
                    print(f"Direct message failed to {recipient_topic} with code: {result.rc}")
        else:
            # Fallback: broadcast to all topics with recipient-specific topics
            if debug:
                print(f"No known topic for recipient {destination_id}, broadcasting to all regions")
                print(f"Payload size: {len(payload)} bytes")
            
            # Convert destination_id to hex format for topic
            recipient_hex = '!' + hex(destination_id)[2:]
            
            # Publish to all root topics with recipient's node ID
            for i, root_topic in enumerate(root_topics):
                direct_topic = root_topic + channel + "/" + recipient_hex
                if debug:
                    print(f"Publishing direct message to topic {i+1}/{len(root_topics)}: {direct_topic}")
                
                result = client.publish(direct_topic, payload)
                
                if debug:
                    print(f"MQTT publish result for {direct_topic}: {result.rc}")
                    if result.rc == 0:
                        print(f"Direct message published successfully to {direct_topic}")
                    else:
                        print(f"Direct message failed to {direct_topic} with code: {result.rc}")
                
                # Add small delay between publications to avoid overwhelming the broker
                if i < len(root_topics) - 1:  # Don't delay after the last topic
                    time.sleep(0.1)


def encrypt_message(channel, key, mesh_packet, encoded_message):
    """Encrypt a message."""
    if debug:
        print("encrypt_message")

    if key == "AQ==":
        key = "1PG7OiApB1nwvP+rz05pAQ=="

    mesh_packet.channel = generate_hash(channel, key)
    key_bytes = base64.b64decode(key.encode('ascii'))

    # print (f"id = {mesh_packet.id}")
    nonce_packet_id = mesh_packet.id.to_bytes(8, "little")
    nonce_from_node = node_number.to_bytes(8, "little")
    # Put both parts into a single byte array.
    nonce = nonce_packet_id + nonce_from_node

    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_bytes = encryptor.update(encoded_message.SerializeToString()) + encryptor.finalize()

    return encrypted_bytes


def generate_targeted_mail_notification_packet(destination_id, encoded_message, target_topic):
    """Send a mail notification packet to a specific topic where the node was last seen."""
    global global_message_id
    
    if debug:
        print(f"Generating targeted mail notification packet for destination {destination_id} on topic {target_topic}")
    
    # Create the base mesh packet
    mesh_packet = mesh_pb2.MeshPacket()
    mesh_packet.id = global_message_id
    global_message_id += 1

    setattr(mesh_packet, "from", node_number)
    mesh_packet.to = destination_id
    mesh_packet.want_ack = True  # Request acknowledgment for mail notifications
    mesh_packet.channel = generate_hash(channel, key)
    
    # Use more hops for mail notifications to ensure delivery
    mesh_packet.hop_limit = 3
    mesh_packet.hop_start = 3

    if debug:
        print(f"Targeted mail notification packet: from={node_number}, to={destination_id}, id={mesh_packet.id}, hops={mesh_packet.hop_limit}")

    # Encrypt the message
    if key == "":
        mesh_packet.decoded.CopyFrom(encoded_message)
        if debug:
            print("key is none")
    else:
        mesh_packet.encrypted = encrypt_message(channel, key, mesh_packet, encoded_message)
        if debug:
            print("key present")

    # Create service envelope
    service_envelope = mqtt_pb2.ServiceEnvelope()
    service_envelope.packet.CopyFrom(mesh_packet)
    service_envelope.channel_id = channel
    service_envelope.gateway_id = node_name

    payload = service_envelope.SerializeToString()
    
    if debug:
        print(f"Targeted mail notification - Sending to specific topic: {target_topic}")
        print(f"Payload size: {len(payload)} bytes")
    
    # Extract the root topic from the target topic (remove the channel/node part)
    # target_topic format: "msh/US/DMV/2/e/LongFast/!abcdb5df"
    # We need to extract "msh/US/DMV/2/e/" part
    topic_parts = target_topic.split('/')
    if len(topic_parts) >= 6:
        root_topic_part = '/'.join(topic_parts[:5]) + '/'
        notification_topic = root_topic_part + channel + "/" + node_name
    else:
        # Fallback: use the primary root topic
        notification_topic = root_topic + channel + "/" + node_name
    
    if debug:
        print(f"Publishing targeted mail notification to: {notification_topic}")
    
    # Add throttling delay before publishing
    time.sleep(mail_notification_throttle_delay)
    
    result = client.publish(notification_topic, payload)
    
    if debug:
        print(f"MQTT publish result for {notification_topic}: {result.rc}")
        if result.rc == 0:
            print(f"Targeted mail notification published successfully to {notification_topic}")
        else:
            print(f"Targeted mail notification failed to {notification_topic} with code: {result.rc}")


def generate_mail_notification_packet(destination_id, encoded_message):
    """Send a mail notification packet to ALL topics to ensure delivery across regions."""
    global global_message_id
    
    if debug:
        print(f"Generating mail notification packet for destination {destination_id}")
    
    # Create the base mesh packet
    mesh_packet = mesh_pb2.MeshPacket()
    mesh_packet.id = global_message_id
    global_message_id += 1

    setattr(mesh_packet, "from", node_number)
    mesh_packet.to = destination_id
    mesh_packet.want_ack = True  # Request acknowledgment for mail notifications
    mesh_packet.channel = generate_hash(channel, key)
    
    # Use more hops for mail notifications to ensure delivery
    mesh_packet.hop_limit = 3
    mesh_packet.hop_start = 3

    if debug:
        print(f"Mail notification packet: from={node_number}, to={destination_id}, id={mesh_packet.id}, hops={mesh_packet.hop_limit}")

    # Encrypt the message
    if key == "":
        mesh_packet.decoded.CopyFrom(encoded_message)
        if debug:
            print("key is none")
    else:
        mesh_packet.encrypted = encrypt_message(channel, key, mesh_packet, encoded_message)
        if debug:
            print("key present")

    # Create service envelope
    service_envelope = mqtt_pb2.ServiceEnvelope()
    service_envelope.packet.CopyFrom(mesh_packet)
    service_envelope.channel_id = channel
    service_envelope.gateway_id = node_name

    payload = service_envelope.SerializeToString()
    
    if debug:
        print(f"Mail notification - Broadcasting to all topics")
        print(f"Payload size: {len(payload)} bytes")
    
    # Publish to ALL root topics to ensure delivery regardless of recipient's region
    if debug:
        print(f"Root topics configured: {root_topics}")
        print(f"Channel: {channel}, Node name: {node_name}")
    
    for i, root_topic in enumerate(root_topics):
        notification_topic = root_topic + channel + "/" + node_name
        if debug:
            print(f"Publishing mail notification to topic {i+1}/{len(root_topics)}: {notification_topic}")
        
        # Add throttling delay between each topic publication
        if i > 0:  # Don't delay before the first topic
            time.sleep(mail_notification_throttle_delay)
        
        result = client.publish(notification_topic, payload)
        
        if debug:
            print(f"MQTT publish result for {notification_topic}: {result.rc}")
            if result.rc == 0:
                print(f"Mail notification published successfully to {notification_topic}")
            else:
                print(f"Mail notification failed to {notification_topic} with code: {result.rc}")
    
    if debug:
        print(f"Mail notification broadcast complete - published to {len(root_topics)} topics")


def send_ack(destination_id, message_id):
    "Return a meshtastic acknowledgement."""
    if debug:
        print("Sending ACK")

    encoded_message = mesh_pb2.Data()
    encoded_message.portnum = portnums_pb2.ROUTING_APP
    encoded_message.request_id = message_id
    encoded_message.payload = b"\030\000"

    generate_mesh_packet(destination_id, encoded_message)


def send_auto_response(destination_id):
    """Send an automatic welcome menu response to a direct message."""
    global node_number
    
    if debug:
        print("=== WELCOME MENU START ===")
        print(f"Destination ID: {destination_id}")
        print(f"Current node number: {node_number}")
        print(f"Client connected: {client.is_connected()}")

    if not client.is_connected():
        if debug:
            print("Not connected to MQTT broker, skipping welcome menu")
        update_console(f"{format_time(current_time())} >>> Welcome menu skipped - not connected to MQTT broker", tag="info")
        return

    # Create the welcome menu message
    welcome_text = "✉️Welcome to Mesh Mail!✉️"
    
    try:
        if debug:
            print(f"Creating welcome menu message")
        
        # Use the exact same approach as the working test direct message
        node_number = int(node_number_entry.get())
        
        if debug:
            print(f"Updated node number: {node_number}")
        
        # Create message exactly like the working test direct message
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP
        encoded_message.payload = welcome_text.encode("utf-8")
        encoded_message.bitfield = 1
        
        if debug:
            print(f"Message created - portnum: {encoded_message.portnum}, payload length: {len(encoded_message.payload)}")
        
        # Send the message using the same approach as test direct message
        if debug:
            print(f"Calling generate_mesh_packet for destination {destination_id}")
        generate_mesh_packet(destination_id, encoded_message)
        
        # Add GUI feedback
        update_console(f"{format_time(current_time())} >>> Welcome menu sent to {get_name_by_id('short', destination_id)}", tag="info")
        
        if debug:
            print(f"Welcome menu successfully sent to {destination_id}")
            print("=== WELCOME MENU END ===")
            
    except Exception as e:
        error_msg = f"Error sending welcome menu: {str(e)}"
        print(error_msg)
        update_console(f"{format_time(current_time())} >>> {error_msg}", tag="info")
        if debug:
            print("=== WELCOME MENU ERROR ===")


def send_simple_ack(destination_id):
    """Send a simple acknowledgment message to test delivery."""
    if debug:
        print("Sending simple ACK")

    if not client.is_connected():
        if debug:
            print("Not connected to MQTT broker, skipping simple ACK")
        return

    try:
        # Send a very simple message
        simple_text = "ok"
        
        # Use global node_number from config instead of GUI
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP
        encoded_message.payload = simple_text.encode("utf-8")
        encoded_message.bitfield = 1
        
        if debug:
            print(f"Sending simple ACK to {destination_id}: {simple_text}")
        
        generate_mesh_packet(destination_id, encoded_message)
        
        if debug:
            print(f"Simple ACK sent to {destination_id}")
            
    except Exception as e:
        print(f"Error sending simple ACK: {str(e)}")


def send_test_broadcast():
    """Send a test broadcast message to see if broadcast works."""
    if debug:
        print("Sending test broadcast")

    if not client.is_connected():
        if debug:
            print("Not connected to MQTT broker, skipping test broadcast")
        return

    try:
        # Send a test broadcast message
        test_text = "test broadcast"
        
        # Use global node_number from config instead of GUI
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP
        encoded_message.payload = test_text.encode("utf-8")
        encoded_message.bitfield = 1
        
        if debug:
            print(f"Sending test broadcast: {test_text}")
        
        generate_mesh_packet(BROADCAST_NUM, encoded_message)
        
        if debug:
            print("Test broadcast sent")
            
    except Exception as e:
        print(f"Error sending test broadcast: {str(e)}")


def send_test_direct_message(target_id):
    """Send a test direct message to see if manual direct messages work."""
    if debug:
        print(f"Sending test direct message to {target_id}")

    if not client.is_connected():
        if debug:
            print("Not connected to MQTT broker, skipping test direct message")
        return

    try:
        # Send a test direct message
        test_text = f"Test direct message at {format_time(current_time())}"
        
        # Use global node_number from config instead of GUI
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP
        encoded_message.payload = test_text.encode("utf-8")
        encoded_message.bitfield = 1
        
        if debug:
            print(f"Sending test direct message to {target_id}: {test_text}")
        
        generate_mesh_packet(target_id, encoded_message)
        
        if debug:
            print(f"Test direct message sent to {target_id}")
            
    except Exception as e:
        print(f"Error sending test direct message: {str(e)}")


def send_auto_response_via_test_function(target_id):
    """Send a random fortune using the exact same approach as the working test direct message."""
    if debug:
        print(f"Sending fortune via test function to {target_id}")

    if not client.is_connected():
        if debug:
            print("Not connected to MQTT broker, skipping fortune")
        return

    try:
        # Read fortunes from file
        with open('fortunes.txt', 'r', encoding='utf-8') as f:
            fortunes = [line.strip() for line in f if line.strip()]
        
        if not fortunes:
            fortune_text = "No fortunes available at this time."
            if debug:
                print("No fortunes found in file")
        else:
            # Pick a random fortune
            fortune_text = random.choice(fortunes)
            if debug:
                print(f"Selected fortune: {fortune_text}")
        
        # Use global node_number from config instead of GUI
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP
        encoded_message.payload = fortune_text.encode("utf-8")
        encoded_message.bitfield = 1
        
        if debug:
            print(f"Sending fortune to {target_id}: {fortune_text}")
        
        generate_mesh_packet(target_id, encoded_message)
        
        if debug:
            print(f"Fortune sent to {target_id}")
            
    except FileNotFoundError:
        error_msg = "Fortune file not found."
        print(error_msg)
        if debug:
            print("=== FORTUNE ERROR (FILE NOT FOUND) ===")
    except Exception as e:
        print(f"Error sending fortune: {str(e)}")


def handle_compose_subject(destination_id, subject_input):
    """Handle mail composition - subject input."""
    # Add delay to prevent race conditions
    time.sleep(2.0)
    
    user_state = get_user_state(destination_id)
    
    if user_state and user_state.startswith('composing_to:'):
        recipient_node_id = int(user_state.split(':')[1])
        
        # Store the subject for the next step
        update_user_state(destination_id, f'composing_subject:{recipient_node_id}:{subject_input}')
        
        # Ask for content
        content_prompt = f"Recipient: {recipient_node_id}\nSubject: {subject_input}\nEnter message content:"
        send_direct_message(destination_id, content_prompt)
    else:
        error_msg = "Error: No recipient found. Please start over.\n\nReply [B]ack to return to mail menu."
        send_direct_message(destination_id, error_msg)


def handle_compose_content(destination_id, content_input):
    """Handle mail composition - content input and final storage."""
    # Add delay to prevent race conditions
    time.sleep(2.0)
    
    user_state = get_user_state(destination_id)
    
    if user_state and user_state.startswith('composing_subject:'):
        parts = user_state.split(':')
        recipient_node_id = int(parts[1])
        subject = parts[2]
        
        # Store the mail in the database
        store_mail_in_db(destination_id, recipient_node_id, subject, content_input)
        
        # Send confirmation to sender
        success_msg = f"Mail sent to {recipient_node_id}!\nSubject: {subject}\n\nReply [B]ack to return to mail menu."
        send_direct_message(destination_id, success_msg)
        
        # Notify recipient that they have new mail
        def delayed_mail_notification():
            time.sleep(mail_notification_delay)  # Configurable delay before notifying recipient
            mail_notification = "✉️ You've got mail! ✉️"
            send_mail_notification(recipient_node_id, mail_notification)
        
        # Start the mail notification in a separate thread
        notification_thread = threading.Thread(target=delayed_mail_notification, daemon=True)
        notification_thread.start()
        
        # Reset state
        update_user_state(destination_id, 'mail_menu')
        update_submenu_state(destination_id, 'mail_menu')
    else:
        error_msg = "Error: No recipient/subject found. Please start over.\n\nReply [B]ack to return to mail menu."
        send_direct_message(destination_id, error_msg)


def send_inbox(destination_id, show_instructions=True):
    """Send inbox contents from database."""
    # Get actual mail from database
    mail_list = get_mail_for_node(destination_id, 'inbox')
    
    if mail_list:
        inbox_text = "Inbox:\n\n"
        for i, mail in enumerate(mail_list, 1):
            mail_id, from_node_id, subject, timestamp, is_read = mail
            read_status = "[READ] " if is_read else "[NEW] "
            inbox_text += f"[{i}] {read_status}From {from_node_id}: {subject}\n"
    else:
        inbox_text = "Inbox is empty."
    
    # Set user state to viewing inbox and submenu to inbox
    update_user_state(destination_id, 'viewing_inbox')
    update_submenu_state(destination_id, 'inbox')
    
    # Send inbox contents immediately
    time.sleep(2.0)
    send_direct_message(destination_id, inbox_text)
    time.sleep(2.0)  # Small delay between messages
    
    # Send additional menu message if instructions should be shown, using threaded delay
    if show_instructions:
        def delayed_menu_response():
            time.sleep(2.0)  # Small delay between messages
            menu_text = "[#] read message or [C]ompose new mail"
            send_direct_message(destination_id, menu_text)
        
        # Start the delayed menu response in a separate thread
        menu_thread = threading.Thread(target=delayed_menu_response, daemon=True)
        menu_thread.start()


def send_sent_mail(destination_id):
    """Send sent mail contents from database."""
    # Get actual sent mail from database
    mail_list = get_mail_for_node(destination_id, 'sent')
    
    if mail_list:
        sent_text = "Sent Mail:\n\n"
        for i, mail in enumerate(mail_list, 1):
            mail_id, to_node_id, subject, timestamp = mail
            sent_text += f"[{i}] To {to_node_id}: {subject}\n"
        sent_text += "\nReply with mail number to read, or [B]ack to return to mail menu."
    else:
        sent_text = "No sent mail.\n\nReply [B]ack to return to mail menu."
    
    # Set user state to viewing sent mail and submenu to sent
    update_user_state(destination_id, 'viewing_sent')
    update_submenu_state(destination_id, 'sent')
    
    # Add a small delay to avoid race conditions
    time.sleep(2.0)
    send_direct_message(destination_id, sent_text)


def send_delete_mail_menu(destination_id):
    """Send delete mail menu."""
    delete_menu = """Delete Mail:
Select mail to delete from inbox or sent mail.
[I]nbox - Delete from inbox
[S]ent - Delete from sent mail
[B]ack - Return to mail menu

Reply with option letter."""
    
    # Set user state to delete mail menu and submenu to delete_mail
    update_user_state(destination_id, 'delete_mail_menu')
    update_submenu_state(destination_id, 'delete_mail')
    
    # Add a small delay to avoid race conditions
    time.sleep(2.0)
    send_direct_message(destination_id, delete_menu)


def send_delete_inbox_list(destination_id):
    """Send list of inbox messages for deletion."""
    # Get actual mail from database
    mail_list = get_mail_for_node(destination_id, 'inbox')
    
    if mail_list:
        delete_text = "Select inbox message to delete:\n\n"
        for i, mail in enumerate(mail_list, 1):
            mail_id, from_node_id, subject, timestamp, is_read = mail
            read_status = "[READ] " if is_read else "[NEW] "
            delete_text += f"[{i}] {read_status}From {from_node_id}: {subject}\n"
        delete_text += "\nReply with mail number to delete, or [B]ack to return to delete menu."
    else:
        delete_text = "Inbox is empty.\n\nReply [B]ack to return to delete menu."
    
    # Set user state to viewing inbox for deletion and submenu to delete_inbox
    update_user_state(destination_id, 'deleting_inbox')
    update_submenu_state(destination_id, 'delete_inbox')
    
    # Add a small delay to avoid race conditions
    time.sleep(2.0)
    send_direct_message(destination_id, delete_text)


def send_delete_sent_list(destination_id):
    """Send list of sent messages for deletion."""
    # Get actual sent mail from database
    mail_list = get_mail_for_node(destination_id, 'sent')
    
    if mail_list:
        delete_text = "Select sent message to delete:\n\n"
        for i, mail in enumerate(mail_list, 1):
            mail_id, to_node_id, subject, timestamp = mail
            delete_text += f"[{i}] To {to_node_id}: {subject}\n"
        delete_text += "\nReply with mail number to delete, or [B]ack to return to delete menu."
    else:
        delete_text = "No sent mail.\n\nReply [B]ack to return to delete menu."
    
    # Set user state to viewing sent for deletion and submenu to delete_sent
    update_user_state(destination_id, 'deleting_sent')
    update_submenu_state(destination_id, 'delete_sent')
    
    # Add a small delay to avoid race conditions
    time.sleep(2.0)
    send_direct_message(destination_id, delete_text)


def delete_mail_by_number(destination_id, mail_type, mail_number):
    """Delete a specific mail message by its number in the list."""
    # Get mail list to find the correct mail ID
    mail_list = get_mail_for_node(destination_id, mail_type)
    
    if not mail_list or mail_number < 1 or mail_number > len(mail_list):
        error_msg = f"Mail {mail_number} not found in {mail_type}."
        send_direct_message(destination_id, error_msg)
        return
    
    # Get the mail ID from the list
    mail_id = mail_list[mail_number - 1][0]  # First column is the mail ID
    
    # Get mail details for confirmation message
    mail_data = get_mail_content(mail_id)
    
    if mail_data:
        # Delete the mail
        delete_mail(mail_id)
        
        # Send confirmation
        if mail_type == "inbox":
            success_msg = f"Deleted inbox mail {mail_number}:\nFrom: {mail_data['from_node_id']}\nSubject: {mail_data['subject']}\n\nReply [B]ack to return to delete menu."
        else:  # sent
            success_msg = f"Deleted sent mail {mail_number}:\nTo: {mail_data['to_node_id']}\nSubject: {mail_data['subject']}\n\nReply [B]ack to return to delete menu."
        
        # Reset state to delete mail menu
        update_user_state(destination_id, 'delete_mail_menu')
        update_submenu_state(destination_id, 'delete_mail')
        
        send_direct_message(destination_id, success_msg)
    else:
        error_msg = f"Mail {mail_number} content not found."
        send_direct_message(destination_id, error_msg)


def handle_delete_mail_selection(destination_id, selection):
    """Handle delete mail menu selection."""
    selection = selection.strip().upper()
    
    if debug:
        print(f"Handling delete mail selection '{selection}' from {destination_id}")
    
    if selection == 'I':
        send_delete_inbox_list(destination_id)
    elif selection == 'S':
        send_delete_sent_list(destination_id)
    elif selection == 'B':
        send_mail_menu(destination_id)
    else:
        # Invalid selection - send help
        help_text = """Invalid delete option. Available options:
[I]nbox - Delete from inbox
[S]ent - Delete from sent mail
[B]ack - Return to mail menu

Reply with option letter."""
        # Add a small delay to avoid race conditions
        time.sleep(2.0)
        send_direct_message(destination_id, help_text)


def send_mail_content(destination_id, mail_type, mail_number):
    """Send the content of a specific mail message from database."""
    # Get mail list to find the correct mail ID
    mail_list = get_mail_for_node(destination_id, mail_type)
    
    if not mail_list or mail_number < 1 or mail_number > len(mail_list):
        # Invalid mail number - send error message
        mail_text = f"Invalid choice. Please select a number between 1 and {len(mail_list) if mail_list else 0}."
        # Don't set viewing_mail state for invalid selections
        # Add a small delay to avoid race conditions
        time.sleep(2.0)
        send_direct_message(destination_id, mail_text)
        return
    
    # Get the mail ID from the list
    mail_id = mail_list[mail_number - 1][0]  # First column is the mail ID
    
    # Get the full mail content
    mail_data = get_mail_content(mail_id)
    
    if mail_data:
        if mail_type == "inbox":
            mail_text = f"Mail {mail_number} (Inbox):\nFrom: {mail_data['from_node_id']}\nSubject: {mail_data['subject']}\nTime: {mail_data['timestamp']}\n\n{mail_data['content']}\n\n[R]eply [D]elete or [B]ack to inbox"
        else:  # sent
            mail_text = f"Mail {mail_number} (Sent):\nTo: {mail_data['to_node_id']}\nSubject: {mail_data['subject']}\nTime: {mail_data['timestamp']}\n\n{mail_data['content']}\n\nReply [B]ack to return to sent mail or [D]elete to delete this message."
        
        # Set user state to viewing this specific mail (only for valid mail)
        update_user_state(destination_id, f'viewing_mail:{mail_type}:{mail_id}')
        update_submenu_state(destination_id, 'viewing_mail')
    else:
        mail_text = f"Mail {mail_number} content not found."
    
    # Add a small delay to avoid race conditions
    time.sleep(2.0)
    send_direct_message(destination_id, mail_text)


#################################
# Database Handling

def setup_db():
    """Setup database tables."""
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"
        node_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodes"
        nodeinfo_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"
        mail_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_mail"
        bulletin_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_bulletin_boards"
        post_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_posts"
        
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            
            # Create messages table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}
                                (time TEXT, sender_short_name TEXT, text_payload TEXT, message_id TEXT, is_encrypted INTEGER)''')
            
            # Create nodes table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {node_table_name}
                                (node_id TEXT PRIMARY KEY, long_name TEXT, short_name TEXT, last_seen TEXT)''')
            
            # Create nodeinfo table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {nodeinfo_table_name}
                                (user_id TEXT PRIMARY KEY, long_name TEXT, short_name TEXT, hw_model INTEGER)''')
            
            # Create mail table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {mail_table_name}
                                (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                 from_node_id TEXT NOT NULL,
                                 to_node_id TEXT NOT NULL,
                                 subject TEXT NOT NULL,
                                 content TEXT NOT NULL,
                                 timestamp TEXT NOT NULL,
                                 is_read INTEGER DEFAULT 0,
                                 is_deleted INTEGER DEFAULT 0)''')
            
            # Create bulletin boards table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {bulletin_table_name}
                                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 name TEXT NOT NULL UNIQUE,
                                 description TEXT,
                                 created_at TEXT NOT NULL,
                                 is_active INTEGER DEFAULT 1)''')
            
            # Create posts table
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {post_table_name}
                                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 board_id INTEGER NOT NULL,
                                 author_node_id TEXT NOT NULL,
                                 author_name TEXT NOT NULL,
                                 subject TEXT NOT NULL,
                                 content TEXT NOT NULL,
                                 timestamp TEXT NOT NULL,
                                 is_deleted INTEGER DEFAULT 0,
                                 FOREIGN KEY (board_id) REFERENCES {bulletin_table_name}(id))''')
            
            db_connection.commit()
            if debug:
                print("Database tables created/verified")
                
    except sqlite3.Error as e:
        print(f"SQLite error in setup_db: {e}")
    finally:
        db_connection.close()


def store_mail_in_db(from_node_id, to_node_id, subject, content):
    """Store a mail message in the database."""
    try:
        mail_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_mail"
        
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            
            timestamp = current_time()
            db_cursor.execute(f'''INSERT INTO {mail_table_name} 
                                (from_node_id, to_node_id, subject, content, timestamp, is_read, is_deleted)
                                VALUES (?, ?, ?, ?, ?, 0, 0)''',
                            (str(from_node_id), str(to_node_id), subject, content, timestamp))
            
            db_connection.commit()
            if debug:
                print(f"Mail stored: from {from_node_id} to {to_node_id}, subject: {subject}")
                
    except sqlite3.Error as e:
        print(f"SQLite error in store_mail_in_db: {e}")
    finally:
        db_connection.close()


def get_mail_for_node(node_id, mail_type='inbox'):
    """Get mail for a specific node (inbox or sent)."""
    try:
        mail_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_mail"
        
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            
            if mail_type == 'inbox':
                # Get mail sent TO this node
                db_cursor.execute(f'''SELECT id, from_node_id, subject, timestamp, is_read 
                                    FROM {mail_table_name} 
                                    WHERE to_node_id = ? AND is_deleted = 0 
                                    ORDER BY timestamp DESC''', (str(node_id),))
            else:  # sent
                # Get mail sent FROM this node
                db_cursor.execute(f'''SELECT id, to_node_id, subject, timestamp 
                                    FROM {mail_table_name} 
                                    WHERE from_node_id = ? AND is_deleted = 0 
                                    ORDER BY timestamp DESC''', (str(node_id),))
            
            mail_list = db_cursor.fetchall()
            if debug:
                print(f"Retrieved {len(mail_list)} {mail_type} messages for node {node_id}")
            
            return mail_list
                
    except sqlite3.Error as e:
        print(f"SQLite error in get_mail_for_node: {e}")
        return []
    finally:
        db_connection.close()


def get_mail_content(mail_id):
    """Get the full content of a specific mail message."""
    try:
        mail_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_mail"
        
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            
            db_cursor.execute(f'''SELECT from_node_id, to_node_id, subject, content, timestamp 
                                FROM {mail_table_name} 
                                WHERE id = ? AND is_deleted = 0''', (mail_id,))
            
            mail_data = db_cursor.fetchone()
            
            if mail_data:
                # Mark as read if it's in inbox
                db_cursor.execute(f'''UPDATE {mail_table_name} 
                                    SET is_read = 1 
                                    WHERE id = ?''', (mail_id,))
                db_connection.commit()
                
                if debug:
                    print(f"Retrieved mail content for mail ID {mail_id}")
                
                return {
                    'from_node_id': mail_data[0],
                    'to_node_id': mail_data[1],
                    'subject': mail_data[2],
                    'content': mail_data[3],
                    'timestamp': mail_data[4]
                }
            else:
                return None
                
    except sqlite3.Error as e:
        print(f"SQLite error in get_mail_content: {e}")
        return None
    finally:
        db_connection.close()


def delete_mail(mail_id):
    """Mark a mail message as deleted."""
    try:
        mail_table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_mail"
        
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
            
            db_cursor.execute(f'''UPDATE {mail_table_name} 
                                SET is_deleted = 1 
                                WHERE id = ?''', (mail_id,))
            
            db_connection.commit()
            if debug:
                print(f"Marked mail ID {mail_id} as deleted")
                
    except sqlite3.Error as e:
        print(f"SQLite error in delete_mail: {e}")
    finally:
        db_connection.close()


def maybe_store_nodeinfo_in_db(info):
    """Save nodeinfo in sqlite unless that record is already there."""

    if debug:
        print("node info packet received: Checking for existing entry in DB")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"

    try:
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Check if a record with the same user_id already exists
            existing_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE user_id=?', (info.id,)).fetchone()

            if existing_record is None:
                if debug:
                    print("no record found, adding node to db")
                # No existing record, insert the new record
                db_cursor.execute(f'''
                    INSERT INTO {table_name} (user_id, long_name, short_name)
                    VALUES (?, ?, ?)
                ''', (info.id, info.long_name, info.short_name))
                db_connection.commit()

                # Fetch the new record
                new_record = db_cursor.execute(f'SELECT user_id, short_name, long_name FROM {table_name} WHERE user_id=?', (info.id,)).fetchone()
                new_node = Node(*new_record)

                # Display the new record in the nodeinfo_window widget
                # This inserts add the end, which breaks the sorting we start with. 
                if debug:
                    print(f"New node added: {new_node.node_list_disp}")
                
                # Deliver mail to new node
                deliver_mail_to_node(info.id)
                
            else:
                # Check if long_name or short_name is different, update if necessary
                if existing_record[1] != info.long_name or existing_record[2] != info.short_name:
                    if debug:
                        print("updating existing record in db")
                    db_cursor.execute(f'''
                        UPDATE {table_name}
                        SET long_name=?, short_name=?
                        WHERE user_id=?
                    ''', (info.long_name, info.short_name, info.id))
                    db_connection.commit()

                    # Fetch the updated record
                    updated_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE user_id=?', (info.id,)).fetchone()

                    # Display the updated record in the nodeinfo_window widget
                    # This appends the record to the end, which breaks sorting
                    if debug:
                        print(f"Node updated: {updated_record[0]}, {updated_record[1]}, {updated_record[2]}")
                    
                    # Deliver mail to returning node
                    deliver_mail_to_node(info.id)

    except sqlite3.Error as e:
        print(f"SQLite error in maybe_store_nodeinfo_in_db: {e}")

    finally:
        db_connection.close()


def deliver_mail_to_node(node_id):
    """Deliver mail to a node when they connect."""
    try:
        # Get unread mail for this node
        mail_list = get_mail_for_node(node_id, 'inbox')
        unread_mail = [mail for mail in mail_list if not mail[4]]  # mail[4] is is_read
        
        if unread_mail:
            if debug:
                print(f"Delivering {len(unread_mail)} unread mail messages to node {node_id}")
            
            # Send mail notification
            mail_count = len(unread_mail)
            notification = f"You have {mail_count} unread mail message{'s' if mail_count > 1 else ''}.\n\nReply [M]ail to check your inbox."
            
            # Add a small delay to avoid race conditions
            time.sleep(2.0)
            send_direct_message(node_id, notification)
        else:
            if debug:
                print(f"No unread mail for node {node_id}")
                
    except Exception as e:
        if debug:
            print(f"Error delivering mail to node {node_id}: {str(e)}")


def maybe_store_position_in_db(node_id, position, rssi=None):
    """Save position if we have no position for this node_id or the timestamp is newer than the record we have stored."""

    # Must have at least a lat/lon
    if position.latitude_i != 0 and position.longitude_i != 0:

        rssi_string = ", RSSI: " + str(rssi) if rssi else ""
        if print_position_report:
            print("From: " + get_name_by_id("short", node_id) +
                ", lat: " + str(round(position.latitude_i * 1e-7, 7)) +
                ", lon: " + str(round(position.longitude_i * 1e-7, 7)) +
                ", alt: " + str(position.altitude) +
                ", PDOP: " + str(position.PDOP) +
                ", speed: " + str(position.ground_speed) +
                ", track: " + str(position.ground_track) +
                ", sats: " + str(position.sats_in_view) +
                rssi_string)

        # Convert from integer lat/lon format to decimal format.
        latitude = position.latitude_i * 1e-7
        longitude = position.longitude_i * 1e-7

        # Get the best timestamp we can, starting with local time.
        timestamp = time.gmtime()
        # Then, try the timestamp from the position protobuf.
        if position.timestamp > 0:
            timestamp = time.gmtime(position.timestamp)
        # Then, try the time from the position protobuf.
        if position.time > 0:
            timestamp = time.gmtime(position.time)
        # Convert timestamp to datetime for database use
        timestamp = datetime.fromtimestamp(mktime(timestamp))

        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_positions"

        try:
            with sqlite3.connect(db_file_path) as db_connection:
                db_cursor = db_connection.cursor()

                # Check for an existing entry for the timestamp; this indicates a position that has bounced around the mesh.
                existing_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE node_id=?', (node_id,)).fetchone()

                # Insert a new record if none exists yet.
                if existing_record is None:
                    db_cursor.execute(f'''
                        INSERT INTO {table_name} (node_id, short_name, timestamp, latitude, longitude)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (node_id, get_name_by_id("short", node_id), timestamp, latitude, longitude))
                    db_connection.commit()
                    return

                if timestamp > datetime.strptime(existing_record[2], "%Y-%m-%d %H:%M:%S"):
                    db_cursor.execute(f'''
                        UPDATE {table_name}
                        SET short_name=?, timestamp=?, latitude=?, longitude=?
                        WHERE node_id=?
                    ''', (get_name_by_id("short", node_id), timestamp, latitude, longitude, node_id))
                    db_connection.commit()
                else:
                    if debug:
                        print("Rejecting old position record")

        except sqlite3.Error as e:
            print(f"SQLite error in maybe_store_position_in_db: {e}")

        finally:
            db_connection.close()


def insert_message_to_db(time, sender_short_name, text_payload, message_id, is_encrypted):
    """Save a meshtastic message to sqlite storage."""

    if debug:
        print("insert_message_to_db")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

    try:
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Strip newline characters and insert the message into the messages table
            formatted_message = text_payload.strip()
            db_cursor.execute(f'INSERT INTO {table_name} (time, sender_short_name, text_payload, message_id, is_encrypted) VALUES (?,?,?,?,?)',
                              (time, sender_short_name, formatted_message, message_id, is_encrypted))
            db_connection.commit()

    except sqlite3.Error as e:
        print(f"SQLite error in insert_message_to_db: {e}")

    finally:
        db_connection.close()





def erase_nodedb():
    """Erase all stored nodeinfo in sqlite and on display in the gui."""

    if debug:
        print("erase_nodedb")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"

    confirmed = console_confirmation(f"Are you sure you want to erase the database: {db_file_path} for channel {channel}?")

    if confirmed:
        try:
            with sqlite3.connect(db_file_path) as db_connection:
                db_cursor = db_connection.cursor()

                # Clear all records from the database
                db_cursor.execute(f'DELETE FROM {table_name}')
                db_connection.commit()

        except sqlite3.Error as e:
            print(f"SQLite error in erase_nodedb: {e}")

        finally:
            db_connection.close()

            # Clear the display
            if debug:
                print("Node database cleared")
            update_console(f"{format_time(current_time())} >>> Node database for channel {channel} erased successfully.", tag="info")
    else:
        update_console(f"{format_time(current_time())} >>> Node database erase for channel {channel} cancelled.", tag="info")


def erase_messagedb():
    """Erase all stored messages in sqlite and on display in the gui."""

    if debug:
        print("erase_messagedb")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

    confirmed = console_confirmation(f"Are you sure you want to erase the message history of: {db_file_path} for channel {channel}?")

    if confirmed:
        try:
            with sqlite3.connect(db_file_path) as db_connection:
                db_cursor = db_connection.cursor()

                # Clear all records from the database
                db_cursor.execute(f'DELETE FROM {table_name}')
                db_connection.commit()

        except sqlite3.Error as e:
            print(f"SQLite error in erase_messagedb: {e}")

        finally:
            db_connection.close()

            # Clear the display
            if debug:
                print("Message history cleared")
            update_console(f"{format_time(current_time())} >>> Message history for channel {channel} erased successfully.", tag="info")
    else:
        update_console(f"{format_time(current_time())} >>> Message history erase for channel {channel} cancelled.", tag="info")


#################################
# MQTT Server

def connect_mqtt():
    """Connect to the MQTT server."""

    if "tls_configured" not in connect_mqtt.__dict__:          #Persistent variable to remember if we've configured TLS yet
        connect_mqtt.tls_configured = False

    if debug:
        print("connect_mqtt")
    global mqtt_broker, mqtt_port, mqtt_username, mqtt_password, root_topic, root_topics, channel, node_number, db_file_path, key
    if not client.is_connected():
        try:
            # Use configuration values directly instead of GUI entries
            if ':' in mqtt_broker:
                mqtt_broker,mqtt_port = mqtt_broker.split(':')
                mqtt_port = int(mqtt_port)

            if key == "AQ==":
                if debug:
                    print("key is default, expanding to AES128")
                key = "1PG7OiApB1nwvP+rz05pAQ=="

            if not move_text_up(): # copy ID to Number and test for 8 bit hex
                return

            padded_key = key.ljust(len(key) + ((4 - (len(key) % 4)) % 4), '=')
            replaced_key = padded_key.replace('-', '+').replace('_', '/')
            key = replaced_key

            if debug:
                print (f"padded & replaced key = {key}")

            setup_db()

            client.username_pw_set(mqtt_username, mqtt_password)
            if mqtt_port == 8883 and connect_mqtt.tls_configured is False:
                client.tls_set(ca_certs="cacert.pem", tls_version=ssl.PROTOCOL_TLSv1_2)
                client.tls_insecure_set(False)
                connect_mqtt.tls_configured = True
            client.connect(mqtt_broker, mqtt_port, 60)
            update_console(f"{format_time(current_time())} >>> Connecting to MQTT broker at {mqtt_broker}...", tag="info")

        except Exception as e:
            update_console(f"{format_time(current_time())} >>> Failed to connect to MQTT broker: {str(e)}", tag="info")

        update_node_list()
    elif client.is_connected() and channel != channel:  # Fixed comparison
        print("Channel has changed, disconnect and reconnect")
        if auto_reconnect:
            print("auto_reconnect disconnecting from MQTT broker")
            disconnect_mqtt()
            time.sleep(auto_reconnect_delay)
            print("auto_reconnect connecting to MQTT broker")
            connect_mqtt()

    else:
        update_console(f"{format_time(current_time())} >>> Already connected to {mqtt_broker}", tag="info")


def disconnect_mqtt():
    """Disconnect from the MQTT server."""

    if debug:
        print("disconnect_mqtt")
    if client.is_connected():
        client.disconnect()
        update_console(f"{format_time(current_time())} >>> Disconnected from MQTT broker", tag="info")
        # Clear the display
        if debug:
            print("Node list cleared")
    else:
        update_console("Already disconnected", tag="info")


def on_connect(client, userdata, flags, reason_code, properties):		# pylint: disable=unused-argument
    """?"""

    set_topic()

    if debug:
        print("on_connect")
        if client.is_connected():
            print("client is connected")

    if reason_code == 0:
        load_message_history_from_db()
        if debug:
            print(f"Subscribe Topics are: {subscribe_topics}")
        
        # Subscribe to all topics
        for topic in subscribe_topics:
            client.subscribe(topic)
            if debug:
                print(f"Subscribed to: {topic}")
        
        topic_list = ", ".join([topic.replace(f"{channel}/#", f"{channel}") for topic in subscribe_topics])
        message = f"{format_time(current_time())} >>> Connected to {mqtt_broker} on topics {topic_list} as {'!' + hex(node_number)[2:]}"
        update_console(message, tag="info")
        send_node_info(BROADCAST_NUM, want_response=False)

        if lon and lat:  # Use config values instead of GUI
            send_position(BROADCAST_NUM)

    else:
        message = f"{format_time(current_time())} >>> Failed to connect to MQTT broker with result code {str(reason_code)}"
        update_console(message, tag="info")


def on_disconnect(client, userdata, flags, reason_code, properties):		# pylint: disable=unused-argument
    """?"""

    if debug:
        print("on_disconnect")
    if reason_code != 0:
        message = f"{format_time(current_time())} >>> Disconnected from MQTT broker with result code {str(reason_code)}"
        update_console(message, tag="info")
        if auto_reconnect is True:
            print("attempting to reconnect in " + str(auto_reconnect_delay) + " second(s)")
            time.sleep(auto_reconnect_delay)
            connect_mqtt()


############################
# Console Functions

def update_node_list():
    """Print node list to console instead of GUI."""
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_nodeinfo"

        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Fetch all nodes from the database
            nodes = db_cursor.execute(f'SELECT user_id, short_name, long_name FROM {table_name} ORDER BY short_name COLLATE NOCASE ASC, long_name COLLATE NOCASE ASC').fetchall()

            # Print each node to console if debug is enabled
            if debug:
                print("Node list:")
                for node_record in nodes:
                    node = Node(*node_record)
                    print(f"  {node.node_list_disp}")

    except sqlite3.Error as e:
        print(f"SQLite error in update_node_list: {e}")

    finally:
        db_connection.close()


def load_message_history_from_db():
    """Load previously stored messages from sqlite - console version."""
    if debug:
        print("load_message_history_from_db")

    table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(channel) + "_messages"

    try:
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Fetch recent messages from the database (limit to last 10 for console)
            messages = db_cursor.execute(f'SELECT time, sender_short_name, text_payload, is_encrypted FROM {table_name} ORDER BY time DESC LIMIT 10').fetchall()

            if debug and messages:
                print("Recent message history:")
                for message in reversed(messages):  # Show in chronological order
                    timestamp = format_time(message[0])
                    emoji = encrypted_emoji if message[3] == 1 else ""
                    print(f"  {timestamp} {emoji}{message[1]}: {message[2]}")

    except sqlite3.Error as e:
        print(f"SQLite error in load_message_history_from_db: {e}")

    finally:
        db_connection.close()


def move_text_up():
    """Validate node ID - console version."""
    # For console version, just validate that node_number is valid hex
    node_id_str = '!' + hex(node_number)[2:]
    if not is_valid_hex(node_id_str, 8, 8):
        print("Not valid Hex")
        return False
    else:
        return True


def move_text_down():
    """Convert node number to hex - console version."""
    # For console version, just return the hex representation
    node_id_str = '!' + hex(node_number)[2:]
    if not is_valid_hex(node_id_str, 8, 8):
        print("Not valid Hex")
        return False
    else:
        return True


def mqtt_thread():
    """Function to run the MQTT client loop in a separate thread."""
    if debug:
        print("MQTT Thread")
        if client.is_connected():
            print("client connected")
        else:
            print("client not connected")
    while True:
        client.loop()


def send_node_info_periodically() -> None:
    """Function to broadcast NodeInfo in a separate thread."""
    while True:
        if client.is_connected():
            send_node_info(BROADCAST_NUM, want_response=False)

            if lon and lat:  # Use config values instead of GUI
                send_position(BROADCAST_NUM)

        time.sleep(node_info_interval_minutes * 60)  # Convert minutes to seconds


def on_exit():
    """Function to be called when the application is closed."""
    if client.is_connected():
        client.disconnect()
        print("client disconnected")
    
    # Save configuration before exiting
    save_config()
    
    client.loop_stop()


# Global initialization (moved from middle of file)
node_name = '!' + hex(node_number)[2:]
if not is_valid_hex(node_name, 8, 8):
    print('Invalid node name from config: ' + str(node_name))
    sys.exit(1)

global_message_id = random.getrandbits(32)

def update_console(text_payload, tag=None):
    """Print message to console instead of GUI."""
    if debug:
        print(f"[{tag if tag else 'INFO'}] {text_payload}")
    else:
        print(text_payload)


def console_confirmation(message):
    """Simple console confirmation instead of GUI messagebox."""
    response = input(f"{message} (y/n): ").lower().strip()
    return response in ['y', 'yes']


#################################
# Bulletin Board Functions (Placeholder implementations)
#################################
def send_bulletin_boards(destination_id):
    """Send bulletin board list."""
    boards_text = """Bulletin Boards:\n[1] GENERAL\n[2] ANNOUNCEMENTS\n[3] TECH\n[4] TEST\n\nReply with board number to view posts."""
    update_global_state(destination_id, 'bulletin')
    update_submenu_state(destination_id, 'boards')
    update_user_state(destination_id, None)
    time.sleep(2.0)
    send_direct_message(destination_id, boards_text)

def send_board_posts(destination_id, board_number):
    """Send posts for a specific board."""
    boards = ["GENERAL", "ANNOUNCEMENTS", "TECH", "TEST"]
    if 1 <= board_number <= len(boards):
        board_name = boards[board_number - 1]
        posts_text = f"Posts in {board_name}:\n\n[1] Sample Post 1\n[2] Sample Post 2\n[3] Sample Post 3\n\nReply with post number to read, or [B]ack to return to boards."
        update_submenu_state(destination_id, 'posts')
        update_user_state(destination_id, f'viewing_board:{board_name}')
        time.sleep(2.0)
        send_direct_message(destination_id, posts_text)
    else:
        error_text = "Invalid board number.\n\nReply [B]ack to return to boards."
        send_direct_message(destination_id, error_text)

def send_post_content(destination_id, board_name, post_number):
    """Send content of a specific post."""
    if 1 <= post_number <= 3:
        post_text = f"Post {post_number} in {board_name}:\n\nThis is sample content for post {post_number} in the {board_name} board.\n\nReply [B]ack to return to posts."
        update_submenu_state(destination_id, 'viewing_post')
        update_user_state(destination_id, f'viewing_post:{board_name}:{post_number}')
        time.sleep(2.0)
        send_direct_message(destination_id, post_text)
    else:
        error_text = "Invalid post number.\n\nReply [B]ack to return to posts."
        send_direct_message(destination_id, error_text)


############################
# Main Application Startup

if __name__ == "__main__":
    print("Meshtastic MQTT Connect - Console Mode")
    print("======================================")
    print(f"Node: {node_name} ({client_short_name})")
    print(f"MQTT Broker: {mqtt_broker}")
    print(f"Channel: {channel}")
    print(f"Root Topics: {', '.join(root_topics)}")
    print("Starting MQTT client...")

    # Initialize MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="", clean_session=True, userdata=None)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Start background threads
    mqtt_thread = threading.Thread(target=mqtt_thread, daemon=True)
    mqtt_thread.start()

    node_info_timer = threading.Thread(target=send_node_info_periodically, daemon=True)
    node_info_timer.start()

    # Auto-connect to MQTT
    connect_mqtt()

    # Keep the application running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        on_exit()