#!/usr/bin/env python3
"""
generate_data.py - Generate synthetic scientific event data
"""

import csv
import random
import datetime
from pathlib import Path

def generate_events(num_events=100000, output_file='data/raw/events.csv'):
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    start_time = datetime.datetime(2025, 1, 1).timestamp()

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)

        writer.writerow([
            'event_id', 'timestamp', 'sensor_id', 'energy',
            'momentum_x', 'momentum_y', 'momentum_z', 'status'
        ])

        for i in range(num_events):
            event_id = i + 1
            timestamp = start_time + random.uniform(0, 86400)
            sensor_id = random.randint(1, 50)

            energy = random.uniform(0.1, 100) if random.random() < 0.85 else random.uniform(100, 1000)

            momentum_x = random.uniform(-50, 50)
            momentum_y = random.uniform(-50, 50)
            momentum_z = random.uniform(-100, 100)

            r = random.random()
            if r < 0.90:
                status = 'valid'
            elif r < 0.95:
                status = 'noise'
            elif r < 0.98:
                status = 'saturated'
            else:
                status = 'invalid'

            writer.writerow([
                event_id, timestamp, sensor_id, energy,
                momentum_x, momentum_y, momentum_z, status
            ])

    print(f"✓ Generated {num_events} events at {output_file}")

if __name__ == "__main__":
    generate_events()
