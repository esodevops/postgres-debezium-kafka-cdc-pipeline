"""Bounded, atomic consumer observations for the read-only dashboard."""
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class ConsumerTelemetry:
    def __init__(self):
        self.path = Path(os.getenv('CDC_TELEMETRY_FILE', '/app/logs/consumer-state.json'))
        self.events = deque(maxlen=200)
        self.errors = deque(maxlen=20)
        self.last_publish = 0
        self.state = dict(total=0, failures=0, counts=dict(c=0, u=0, d=0, r=0), tables={},
                          status='Connecting to Kafka', error=None, started=now_iso())
        self.publish(force=True)

    def publish(self, force=False):
        if not force and time.monotonic() - self.last_publish < 0.5:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            body = dict(self.state, events=list(self.events), errors=list(self.errors), updated=now_iso())
            temporary = self.path.with_suffix('.tmp')
            temporary.write_text(json.dumps(body))
            temporary.replace(self.path)
            self.last_publish = time.monotonic()
        except OSError:
            logging.getLogger('CDCConsumer').warning('Unable to write consumer telemetry')

    def set_status(self, status, error=None):
        # Raw exception strings may contain connection details; keep those in logs.
        self.state.update(status=status, error='See consumer logs for details.' if error else None)
        if error:
            self.state['failures'] += 1
            self.errors.appendleft(dict(timestamp=now_iso(), category=status,
                                        message='See consumer logs for details.'))
        self.publish(force=True)

    def record(self, table, operation, data, kafka=None):
        self.state['total'] += 1
        counts, tables = self.state['counts'], self.state['tables']
        counts[operation] = counts.get(operation, 0) + 1
        tables[table] = tables.get(table, 0) + 1
        self.state.update(status='Listening for CDC events', error=None)
        self.events.appendleft(dict(id=self.state['total'], table=table, operation=operation,
                                   data=data, received=now_iso(), kafka=kafka or {}))
        self.publish()
