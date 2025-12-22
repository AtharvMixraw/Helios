#!/usr/bin/env python3
"""
parallel_processor.py - Parallel event processing with multiprocessing
"""

import csv
import sys
import time
import json
from pathlib import Path
from multiprocessing import Pool, cpu_count
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
        
        # Count by status
        status = row['status']
        self.status_counts[status] += 1
        
        # Count by sensor
        sensor_id = row['sensor_id']
        self.sensor_counts[sensor_id] += 1
        
        # Energy statistics
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
    
def process_chunk(args):
    """
    Worker function to process a chunk of data
    This will be called in parallel by multiple processes
    """
    file_path, start_row, num_rows, chunk_id = args
    print(f"[Worker {chunk_id}] Starting: rows {start_row} to {start_row + num_rows - 1}")
    start_time = time.time()

    processor = EventProcessor()
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        
        # Skip to start row
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            if i >= start_row + num_rows:
                break
            processor.process_event(row)
    
    elapsed = time.time() - start_time
    results = processor.get_results()
    results['chunk_id'] = chunk_id
    results['processing_time'] = elapsed
    
    print(f"[Worker {chunk_id}] Completed: {results['total']} events in {elapsed:.2f}s")
    
    return results

def merge_results(partial_results):
    """Merge results from all workers"""
    merged = {
        'total': 0,
        'status_counts': defaultdict(int),
        'sensor_counts': defaultdict(int),
        'energy_sum': 0.0,
        'high_energy_events': 0,
        'chunks_processed': len(partial_results),
        'total_processing_time': 0.0
    }
    
    for result in partial_results:
        merged['total'] += result['total']
        merged['energy_sum'] += result['energy_sum']
        merged['high_energy_events'] += result['high_energy_events']
        merged['total_processing_time'] += result['processing_time']
        
        # Merge status counts
        for status, count in result['status_counts'].items():
            merged['status_counts'][status] += count
        
        # Merge sensor counts
        for sensor_id, count in result['sensor_counts'].items():
            merged['sensor_counts'][sensor_id] += count
    
    # Calculate final statistics
    merged['avg_energy'] = merged['energy_sum'] / merged['total'] if merged['total'] > 0 else 0
    merged['status_counts'] = dict(merged['status_counts'])
    merged['sensor_counts'] = dict(merged['sensor_counts'])
    
    return merged

    
def split_into_chunks(total_rows, num_of_workers):
    """Split data into chunks for parallel processing"""
    chunk_size = total_rows // num_of_workers
    chunks = []

    for i in range(num_of_workers):
        start_row = i*chunk_size
        if i == num_of_workers - 1:
            num_rows = total_rows - start_row
        else:
            num_rows = chunk_size
        chunks.append((start_row,num_rows))
    
    return chunks

def count_data_rows(file_path):
    """Count total data rows in CSV (excluding header)"""
    with open(file_path,'r') as f:
        return sum(1 for _ in f) - 1 #subtract header row

def main():
    if len(sys.argv) < 2:
        print("Usage: python parallel_processor.py <input_file> [num_workers]")
        print("Example: python parallel_processor.py data/raw/events.csv 4")
        sys.exit(1)

    input_file = sys.argv[1]
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else cpu_count()

    if not Path(input_file).exists():
        print(f"Error: {input_file} not found")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("PARALLEL EVENT PROCESSING")
    print("="*70)
    print(f"Input file: {input_file}")
    print(f"Number of workers: {num_workers}")
    print(f"Available CPUs: {cpu_count()}")
    print("="*70 + "\n")

    # Count total rows
    print("Counting rows...")
    total_rows = count_data_rows(input_file)
    print(f"Total events to process: {total_rows:,}\n")

    # Split into chunks
    chunks = split_into_chunks(total_rows, num_workers)
    print("Chunk distribution:")
    for i, (start, count) in enumerate(chunks):
        print(f"  Worker {i}: rows {start:,} to {start + count - 1:,} ({count:,} events)")
    print()

    worker_args = [
        (input_file, start, count, i)
        for i, (start, count) in enumerate(chunks)
    ]

    # Run parallel processing
    print("Starting parallel processing...\n")
    overall_start = time.time()
    
    with Pool(processes=num_workers) as pool:
        partial_results = pool.map(process_chunk, worker_args)
    
    overall_elapsed = time.time() - overall_start

    print("\nMerging results...")
    final_results = merge_results(partial_results)
    
    # Save results
    output_dir = Path('data/processed')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'parallel_results.json'
    
    with open(output_file, 'w') as f:
        json.dump(final_results, f, indent=2)
    
    # Display results
    print("\n" + "="*70)
    print("PROCESSING COMPLETE")
    print("="*70)
    print(f"Total events processed: {final_results['total']:,}")
    print(f"Average energy: {final_results['avg_energy']:.2f}")
    print(f"High energy events (>100): {final_results['high_energy_events']:,}")
    print("\nStatus distribution:")
    for status, count in sorted(final_results['status_counts'].items()):
        pct = 100 * count / final_results['total']
        print(f"  {status:>10}: {count:>7,} ({pct:>5.1f}%)")
    print("\nTop 5 sensors by event count:")
    top_sensors = sorted(final_results['sensor_counts'].items(), 
                         key=lambda x: x[1], reverse=True)[:5]
    for sensor_id, count in top_sensors:
        print(f"  Sensor {sensor_id}: {count:,} events")
    
    # Performance metrics
    print(f"\n{'='*70}")
    print("PERFORMANCE METRICS")
    print("="*70)
    print(f"Wall clock time: {overall_elapsed:.2f}s")
    print(f"Total CPU time: {final_results['total_processing_time']:.2f}s")
    print(f"Speedup factor: {final_results['total_processing_time'] / overall_elapsed:.2f}x")
    print(f"Events per second: {final_results['total'] / overall_elapsed:,.0f}")
    print(f"\nResults saved to: {output_file}")
    print("="*70 + "\n")

if __name__ == '__main__':
    main()