#!/usr/bin/env python3
"""
Network Rail Train Movement Data Consumer

This script connects to Network Rail's ActiveMQ feed to consume real-time train movement data.
It processes movement messages and provides structured output for analysis.
"""

import json
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, Any, Optional

import stomp
from pytz import timezone


# Configuration constants
TIMEZONE_LONDON = timezone("Europe/London")
NETWORKRAIL_HOST = "datafeeds.networkrail.co.uk"
NETWORKRAIL_PORT = 61618
TRAIN_MOVEMENT_TOPIC = "/topic/TRAIN_MVT_ALL_TOC"
HEARTBEAT_INTERVAL = 5000
KEEPALIVE_INTERVAL = 5000
SLEEP_INTERVAL = 1

# Message type constants
MESSAGE_TYPE_MOVEMENT = "0003"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('movement_consumer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NetworkRailCredentialsError(Exception):
    """Raised when Network Rail credentials are invalid or missing."""
    pass


class NetworkRailConnectionError(Exception):
    """Raised when connection to Network Rail fails."""
    pass


class MovementMessageProcessor:
    """Processes train movement messages from Network Rail feed."""
    
    def __init__(self):
        self.message_count = 0
        self.error_count = 0
    
    def process_movement_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single movement message and extract relevant information.
        
        Args:
            message: Raw message from the feed
            
        Returns:
            Processed movement data or None if processing fails
        """
        try:
            header = message.get("header", {})
            body = message.get("body", {})
            
            message_type = header.get("msg_type")
            if message_type != MESSAGE_TYPE_MOVEMENT:
                return None
            
            # Extract timestamp and convert to UK time
            timestamp = int(body.get("actual_timestamp", 0)) / 1000
            utc_datetime = datetime.utcfromtimestamp(timestamp)
            uk_datetime = TIMEZONE_LONDON.fromutc(utc_datetime)
            
            # Extract movement details
            movement_data = {
                "message_type": message_type,
                "event_type": body.get("event_type"),
                "toc_id": body.get("toc_id"),
                "variation_status": body.get("variation_status"),
                "uk_datetime": uk_datetime.isoformat(),
                "timestamp": timestamp,
                "processed_at": datetime.now().isoformat()
            }
            
            self.message_count += 1
            return movement_data
            
        except (KeyError, ValueError, TypeError) as e:
            self.error_count += 1
            logger.error(f"Error processing message: {e}")
            return None
    
    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return {
            "messages_processed": self.message_count,
            "errors": self.error_count
        }


class NetworkRailListener(stomp.ConnectionListener):
    """STOMP connection listener for Network Rail feed."""
    
    def __init__(self, connection: stomp.Connection, processor: MovementMessageProcessor):
        self.connection = connection
        self.processor = processor
        self.connected = False
    
    def on_connected(self, frame):
        """Called when connection is established."""
        self.connected = True
        logger.info("Successfully connected to Network Rail feed")
    
    def on_disconnected(self):
        """Called when connection is lost."""
        self.connected = False
        logger.warning("Disconnected from Network Rail feed")
    
    def on_error(self, frame):
        """Called when an error occurs."""
        logger.error(f"STOMP error: {frame.body}")
    
    def on_message(self, frame):
        """Process incoming messages."""
        try:
            headers, message_raw = frame.headers, frame.body
            
            # Acknowledge message receipt
            if "message-id" in headers and "subscription" in headers:
                self.connection.ack(
                    id=headers["message-id"], 
                    subscription=headers["subscription"]
                )
            
            # Parse and process messages
            parsed_body = json.loads(message_raw)
            
            for message in parsed_body:
                processed_data = self.processor.process_movement_message(message)
                if processed_data:
                    self._log_movement(processed_data)
                    
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")
    
    def _log_movement(self, movement_data: Dict[str, Any]):
        """Log processed movement data."""
        logger.info(
            f"Movement: {movement_data['event_type']} | "
            f"TOC: {movement_data['toc_id']} | "
            f"Status: {movement_data['variation_status']} | "
            f"Time: {movement_data['uk_datetime']}"
        )


class NetworkRailConsumer:
    """Main consumer class for Network Rail train movement data."""
    
    def __init__(self, credentials_file: str = "secrets.json"):
        self.credentials_file = Path(credentials_file)
        self.connection = None
        self.processor = MovementMessageProcessor()
        self.running = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
    
    def _load_credentials(self) -> tuple[str, str]:
        """Load Network Rail credentials from file."""
        if not self.credentials_file.exists():
            raise NetworkRailCredentialsError(
                f"Credentials file not found: {self.credentials_file}"
            )
        
        try:
            with open(self.credentials_file) as f:
                credentials = json.load(f)
                feed_username = credentials.get("username")
                feed_password = credentials.get("password")
                
                if not feed_username or not feed_password:
                    raise NetworkRailCredentialsError("Missing username or password in credentials file")
                
                return feed_username, feed_password
                
        except json.JSONDecodeError as e:
            raise NetworkRailCredentialsError(f"Invalid JSON in credentials file: {e}")
        except Exception as e:
            raise NetworkRailCredentialsError(f"Error reading credentials file: {e}")
    
    def connect(self) -> None:
        """Establish connection to Network Rail feed."""
        try:
            username, password = self._load_credentials()
            
            self.connection = stomp.Connection(
                [(NETWORKRAIL_HOST, NETWORKRAIL_PORT)],
                keepalive=True,
                heartbeats=(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL),
            )
            
            # Set up listener
            listener = NetworkRailListener(self.connection, self.processor)
            self.connection.set_listener("", listener)
            
            # Connect to the feed
            self.connection.connect(
                username=username,
                passcode=password,
                wait=True,
                client_id=username,
            )
            
            # Subscribe to train movement topic
            self.connection.subscribe(
                destination=TRAIN_MOVEMENT_TOPIC,
                id=1,
                ack="client-individual",
                activemq_subscriptionName="TRAIN_MVT_ALL_TOC",
            )
            
            logger.info("Successfully subscribed to train movement feed")
            
        except Exception as e:
            raise NetworkRailConnectionError(f"Failed to connect to Network Rail: {e}")
    
    def start(self) -> None:
        """Start consuming messages."""
        try:
            self.connect()
            self.running = True
            
            logger.info("Starting to consume train movement messages...")
            logger.info("Press Ctrl+C to stop")
            
            while self.running and self.connection and self.connection.is_connected():
                sleep(SLEEP_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Stop the consumer and clean up."""
        self.running = False
        
        if self.connection and self.connection.is_connected():
            try:
                self.connection.disconnect()
                logger.info("Disconnected from Network Rail feed")
            except Exception as e:
                logger.error(f"Error during disconnect: {e}")
        
        # Log final statistics
        stats = self.processor.get_stats()
        logger.info(f"Final statistics: {stats}")


def main():
    """Main entry point."""
    try:
        consumer = NetworkRailConsumer()
        consumer.start()
    except NetworkRailCredentialsError as e:
        logger.error(f"Credentials error: {e}")
        sys.exit(1)
    except NetworkRailConnectionError as e:
        logger.error(f"Connection error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
