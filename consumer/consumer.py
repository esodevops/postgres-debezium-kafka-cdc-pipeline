"""Reconstructed from the supplied screenshots; unseen sections are marked."""

import json
import os
import sys
import time
import signal
import logging
import asyncio
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from enum import Enum

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn
from pydantic import BaseModel
from telemetry import ConsumerTelemetry

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'cdc-consumer-group')
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# Topics to subscribe to
CDC_TOPICS = [
    'cdc.customers',
    'cdc.products',
    'cdc.orders',
    'cdc.order_items',
    'cdc.inventory'
]

# Rich Console Setup
console = Console()

# Logging Configuration
# Local-run accommodation: the screenshots use /app/logs/consumer.log.
LOG_FILE = os.getenv('CDC_LOG_FILE', '/app/logs/consumer.log')
if not os.path.isdir(os.path.dirname(LOG_FILE) or '.'):
    LOG_FILE = 'consumer.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger('CDCConsumer')

# Data Models
class OperationType(str, Enum):
    CREATE = 'c'
    UPDATE = 'u'
    DELETE = 'd'
    READ = 'r'  # Snapshot reads


class CDCEvent(BaseModel):
    """Represents a CDC event from Debezium"""
    operation: str
    table: str
    timestamp: Optional[int] = None
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    source_ts_ms: Optional[int] = None


# Event Statistics Tracker
@dataclass
class EventStats:
    """Track event processing statistics"""
    total_events: int = 0
    creates: int = 0
    updates: int = 0
    deletes: int = 0
    reads: int = 0
    errors: int = 0
    events_per_table: Dict[str, int] = field(default_factory=dict)
    recent_events: List[Dict[str, Any]] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    last_event_time: Optional[datetime] = None
    dashboard: Any = field(default=None, repr=False)

    def record_event(self, table: str, operation: str, data: Dict[str, Any], kafka=None):
        """Record a new event"""
        self.total_events += 1
        self.last_event_time = datetime.now()
        if self.dashboard:
            self.dashboard.record(table, operation, data, kafka)

        # Count by operation type
        if operation == 'c':
            self.creates += 1
        elif operation == 'u':
            self.updates += 1
        elif operation == 'd':
            self.deletes += 1
        elif operation == 'r':
            self.reads += 1

        # Count by table
        self.events_per_table[table] = self.events_per_table.get(table, 0) + 1

        # Store recent events (keep last 10)
        self.recent_events.append({
            'table': table,
            'operation': operation,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
        if len(self.recent_events) > 10:
            self.recent_events.pop(0)

    def get_events_per_second(self) -> float:
        """Calculate events per second"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return self.total_events / elapsed if elapsed > 0 else 0


# CDC Event Processor
class CDCEventProcessor:
    """Process CDC events with business logic"""

    def __init__(self, stats: EventStats):
        self.stats = stats
        self.handlers = {
            'customers': self._process_customer_event,
            'products': self._process_product_event,
            'orders': self._process_order_event,
            'order_items': self._process_order_item_event,
            'inventory': self._process_inventory_event,
        }

    def process(self, topic: str, message: Dict[str, Any], kafka=None):
        """Main entry point for processing CDC events"""
        try:
            # Extract table name from topic (cdc.{table_name})
            table = topic.rsplit('.', 1)[-1]

            # Handle the event
            if message is None:
                # Tombstone event (delete marker)
                logger.info(f'Tombstone received for {table}')
                return

            # Extract operation type
            # Compatibility addition: current connector uses op with unwrap disabled.
            message = message.get('payload', message)
            if not isinstance(message, dict):
                raise ValueError('Expected a CDC object')
            operation = message.get('__op', message.get('op'))
            if operation not in ('c', 'u', 'd', 'r'):
                raise ValueError('Unknown CDC operation')

            # Call specific handler if exists
            handler = self.handlers.get(table)
            if handler:
                handler(operation, message)
            # Count successful handling rather than attempts.
            self.stats.record_event(table, operation, message, kafka)
        except Exception as e:
            # Completion: exception block is not visible in the screenshots.
            self.stats.errors += 1
            if self.stats.dashboard:
                self.stats.dashboard.set_status('Processing error', e)
            logger.error(f'Error processing event: {e}', exc_info=True)

    # Completion: the screenshots show these names, but not their bodies.
    def _process_customer_event(self, operation: str, data: Dict[str, Any]):
        logger.info(f'Customer event: operation={operation}')

    def _process_product_event(self, operation: str, data: Dict[str, Any]):
        logger.info(f'Product event: operation={operation}')

    def _process_order_event(self, operation: str, data: Dict[str, Any]):
        logger.info(f'Order event: operation={operation}')

    def _process_order_item_event(self, operation: str, data: Dict[str, Any]):
        logger.info(f'Order item event: operation={operation}')

    def _process_inventory_event(self, operation: str, data: Dict[str, Any]):
        logger.info(f'Inventory event: operation={operation}')


# Completion: only the dashboard return statement is visible in the screenshots.
def create_dashboard(stats):
    stats_table = Table(title='CDC Event Statistics', box=box.ROUNDED)
    stats_table.add_column('Metric', style='cyan')
    stats_table.add_column('Value', justify='right')
    stats_table.add_row('Total Events', str(stats.total_events))
    stats_table.add_row('Creates', str(stats.creates))
    stats_table.add_row('Updates', str(stats.updates))
    stats_table.add_row('Deletes', str(stats.deletes))
    stats_table.add_row('Snapshot Reads', str(stats.reads))
    stats_table.add_row('Errors', str(stats.errors))
    stats_table.add_row('Events/Second', f'{stats.get_events_per_second():.2f}')

    table_stats = Table(title='Events per Table', box=box.ROUNDED)
    table_stats.add_column('Table', style='cyan')
    table_stats.add_column('Events', justify='right')
    for table, count in stats.events_per_table.items():
        table_stats.add_row(table, str(count))
    return stats_table, table_stats


# Main Consumer
class CDCConsumer:
    """Main CDC Consumer class"""

    def __init__(self):
        self.running = True
        self.consumer = None
        self.stats = EventStats()
        self.dashboard = ConsumerTelemetry()
        self.stats.dashboard = self.dashboard
        self.processor = CDCEventProcessor(self.stats)

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        console.print('\nShutdown signal received, closing consumer...')
        self.running = False

    def _wait_for_kafka(self, max_retries: int = 30, delay: int = 5) -> bool:
        """Wait for Kafka to be ready"""
        console.print('Waiting for Kafka to be ready...')

        for attempt in range(max_retries):
            if not self.running:
                return False
            try:
                admin = AdminClient({
                    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
                })
                metadata = admin.list_topics(timeout=5)
                console.print('Kafka is ready!')
                return True
            except KafkaException as e:
                console.print(f'Kafka not ready, attempt {attempt + 1}/{max_retries}...')
                time.sleep(delay)
            except Exception as e:
                logger.warning(f'Connection attempt failed: {e}')
                time.sleep(delay)

        return False

    def _wait_for_topics(self, timeout: int = 120) -> bool:
        """Wait for CDC topics to be created"""
        console.print('Waiting for CDC topics...')

        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self.running:
                return False
            try:
                admin = AdminClient({
                    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
                })
                metadata = admin.list_topics(timeout=10)
                topics = list(metadata.topics.keys())

                # Check if any CDC topics exist
                cdc_topics_found = [t for t in topics if t.startswith('cdc.')]
                if cdc_topics_found:
                    console.print(f'Found CDC topics: {cdc_topics_found}')
                    return True

                console.print('No CDC topics yet, waiting for Debezium...')
            except Exception as e:
                # Completion: this block falls between screenshots.
                logger.warning(f'Topic lookup failed: {e}')
            time.sleep(5)

        console.print('Timeout waiting for topics, starting anyway...')
        return True

    def start(self):
        """Start the consumer"""
        # Print startup banner
        console.print(Panel.fit(
            'Enterprise CDC Consumer\n'
            'Real-time Change Data Capture Processor\n'
            f'Kafka: {KAFKA_BOOTSTRAP_SERVERS}'
        ))

        # Wait for Kafka
        if not self._wait_for_kafka():
            console.print('Failed to connect to Kafka')
            self.dashboard.set_status('Kafka connection failed', True)
            return

        # Wait for topics
        self._wait_for_topics()
        if not self.running:
            return

        # Create consumer
        try:
            self.consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': KAFKA_GROUP_ID,
                'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET,
                'enable.auto.commit': True,
                'client.id': 'cdc-consumer'
            })

            self.consumer.subscribe(CDC_TOPICS)

            console.print(f'Subscribed to topics: {CDC_TOPICS}')
            console.print('Listening for CDC events...\n')
            self.dashboard.set_status('Listening for CDC events')
        except Exception as e:
            console.print(f'Failed to create consumer: {e}')
            self.dashboard.set_status('Consumer failed', e)
            return

        # Main processing loop
        self._consume_loop()

        # Cleanup
        self.consumer.close()
        self.dashboard.set_status('Stopped')
        console.print('Consumer stopped gracefully')

        # Print final stats
        stats_table, table_stats = create_dashboard(self.stats)
        console.print('\n')
        console.print(stats_table)
        console.print(table_stats)

    def _consume_loop(self):
        """Main message consumption loop"""
        while self.running:
            try:
                # Poll for messages (returns single message)
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    self.dashboard.publish()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        logger.error(f'Consumer error: {msg.error()}')
                        self.dashboard.set_status('Kafka error', msg.error())
                        continue

                # Deserialize message value
                try:
                    value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
                    self.processor.process(
                        topic=msg.topic(),
                        message=value,
                        kafka={'topic': msg.topic(), 'partition': msg.partition(),
                               'offset': str(msg.offset()),
                               'timestamp': msg.timestamp()[1]}
                    )
                except json.JSONDecodeError as e:
                    logger.error(f'Failed to decode message: {e}')
                    self.dashboard.set_status('Invalid event JSON', e)
                    continue

                # Periodically print stats
                if self.stats.total_events > 0 and self.stats.total_events % 50 == 0:
                    stats_table, _ = create_dashboard(self.stats)
                    console.print(stats_table)

            except Exception as e:
                logger.error(f'Error in consume loop: {e}', exc_info=True)
                self.dashboard.set_status('Consumer error', e)
                time.sleep(1)


# Entry Point
if __name__ == '__main__':
    consumer = CDCConsumer()
    consumer.start()
