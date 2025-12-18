#!/usr/bin/env python3
"""
worker.py - Worker process for subprocess-based parallel processing
"""

import csv
import sys
import time
import json
from collections import defaultdict

class EventProcessor:
    """Process events and compute statistics"""
    
    def __init__(self):
        self.total = 0
        self.status_counts = defaultdict(int)
        self.sensor_counts = defaultdict(int)
        self.energy_sum = 0.0
        self.high_energy_events = 0
        
    def process_event(self, row):
        """Process a single event"""
        self.total += 1
        
        status = row['status']
        self.status_counts[status] += 1
        
        sensor_id = row['sensor_id']
        self.sensor_counts[sensor_id] += 1
        
        energy = float(row['energy'])
        self.energy_sum += energy
        if energy > 100:
            self.high_energy_events += 1
    
    def get_results(self):
        """Return processing results"""
        return {
            'total': self.total,
            'status_counts': dict(self.status_counts),
            'sensor_counts': dict(self.sensor_counts),
            'energy_sum': self.energy_sum,
            'high_energy_events': self.high_energy_events,
            'avg_energy': self.energy_sum / self.total if self.total > 0 else 0
        }

def main():
    if len(sys.argv) != 6:
        print("Usage: worker.py <input_file> <start_row> <num_rows> <worker_id> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    start_row = int(sys.argv[2])
    num_rows = int(sys.argv[3])
    worker_id = int(sys.argv[4])
    output_file = sys.argv[5]
    
    print(f"[Worker {worker_id}] Starting: processing rows {start_row} to {start_row + num_rows - 1}")
    start_time = time.time()
    
    processor = EventProcessor()
    
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            if i >= start_row + num_rows:
                break
            processor.process_event(row)
    
    elapsed = time.time() - start_time
    results = processor.get_results()
    results['worker_id'] = worker_id
    results['processing_time'] = elapsed
    
    # Save results to file
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"[Worker {worker_id}] Completed: {results['total']} events in {elapsed:.2f}s")
    print(f"[Worker {worker_id}] Results saved to {output_file}")

if __name__ == "__main__":
    main()