## Mesh Mail - A Nodeless MQTT Client that allows users to send messages across MQTT Topcics!

Many thanks to and inspiration from:<br>
 https://github.com/arankwende/meshtastic-mqtt-client & https://github.com/joshpirihi/meshtastic-mqtt & https://github.com/pdxlocations/connect<br>
<b>Powered by Meshtastic.org</b>

### Installation


Then create a virtual environment and install Connect:
```bash
python3 -m venv connect
cd connect && source bin/activate
pip3 install meshtastic paho-mqtt cryptography
git clone https://github.com/Arkwin/Mesh-Mail.git
cd Mesh-Mail
```

### Running the Client
```bash
python mqtt-connect.py
```

### Use 

## Mesh Mail System

This system provides a persistent mail service for Meshtastic users across multiple regions. Users can send and receive mail messages that are stored in a database, allowing for asynchronous communication even when nodes are offline.

### Getting Started with Mail

**Accessing the Mail System:**
Send any direct message to the mail server node (MMX) to receive the welcome menu.

### Using the Mail System

Once you send a message you will be greated with your personal inbox and mail options either enter the mail number to read the message or C to compose a message to someone on any of the connected MQTT topics.

#### Composing Mail
1. Send `C` to enter compose mode
2. Enter recipient node ID in format `!12345678` (hex format)
3. Enter subject line when prompted
4. Enter message content when prompted
5. Mail is automatically sent and recipient receives notification


### Multi-Region Support

The mail system automatically works across different Meshtastic regions:
- **Supported Regions:** DMV, VA, MD, VA/RVA
- **Cross-Region Delivery:** Messages automatically route between regions
- **Smart Routing:** System remembers where users were last seen for efficient delivery

### Configuration

Edit `config.ini` to customize:
- **MQTT Settings:** Broker, credentials, topics
- **Node Information:** Your node number, names, location  
- **Mail Timing:** Notification delays and throttling
- **Debug Options:** Detailed logging and message tracking

### Database Storage

- Mail messages are stored persistently in SQLite database
- Node information is cached for faster lookups
- Message history is maintained for debugging
- Automatic database setup on first run

### Features

- ✅ **Persistent Storage** - Messages saved even when nodes offline
- ✅ **Cross-Region Delivery** - Works across multiple Meshtastic regions  
- ✅ **Smart Notifications** - Users notified of new mail with emoji
- ✅ **Anti-Spam Protection** - Throttling prevents message flooding
- ✅ **Multi-Topic Support** - Automatic routing to correct regional topics
- ✅ **Database Persistence** - All mail stored in SQLite database
- ✅ **Menu Navigation** - Easy-to-use text-based interface
