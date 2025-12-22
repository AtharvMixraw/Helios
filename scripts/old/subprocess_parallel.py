#!/usr/bin/env python3
"""
subprocess_parallel.py - Parallel processing using subprocess
"""

import subprocess
import sys
import time
import json
from pathlib import Path
from collections import defaultdict

def count_data_rows(file_path):
    """Count total data rows in CSV"""
    with open(file_path, 'r') as f:
        return sum(1 for _ in f) - 1

def split_into_chunks(total_rows, num_workers):
    """Split data into chunks"""
    chunk_size = total_rows // num_workers
    chunks = []
    
    for i in range(num_workers):
        start_row = i * chunk_size
        if i == num_workers - 1:
            num_rows = total_rows - start_row
        else:
            num_rows = chunk_size
        chunks.append((start_row, num_rows))
    
    return chunks

def launch_worker(input_file, start_row, num_rows, worker_id, output_dir):
    """Launch a worker process using subprocess"""
    output_file = output_dir / f'chunk_{worker_id}.json'
    
    cmd = [
        'python3', 'worker.py',
        input_file,
        str(start_row),
        str(num_rows),
        str(worker_id),
        str(output_file)
    ]
    
    print(f"[Launcher] Starting worker {worker_id}: rows {start_row} to {start_row + num_rows - 1}")
    
    return subprocess.Popen(cmd)

def merge_results(output_dir, num_workers):
    """Merge results from all worker output files"""
    merged = {
        'total': 0,
        'status_counts': defaultdict(int),
        'sensor_counts': defaultdict(int),
        'energy_sum': 0.0,
        'high_energy_events': 0,
        'chunks_processed': 0,
        'total_processing_time': 0.0
    }
    
    for i in range(num_workers):
        result_file = output_dir / f'chunk_{i}.json'
        
        if not result_file.exists():
            print(f"Warning: {result_file} not found")
            continue
        
        with open(result_file, 'r') as f:
            result = json.load(f)
        
        merged['total'] += result['total']
        merged['energy_sum'] += result['energy_sum']
        merged['high_energy_events'] += result['high_energy_events']
        merged['total_processing_time'] += result['processing_time']
        merged['chunks_processed'] += 1
        
        for status, count in result['status_counts'].items():
            merged['status_counts'][status] += count
        
        for sensor_id, count in result['sensor_counts'].items():
            merged['sensor_counts'][sensor_id] += count
    
    merged['avg_energy'] = merged['energy_sum'] / merged['total'] if merged['total'] > 0 else 0
    merged['status_counts'] = dict(merged['status_counts'])
    merged['sensor_counts'] = dict(merged['sensor_counts'])
    
    return merged

def main():
    if len(sys.argv) < 2:
        print("Usage: python subprocess_parallel.py <input_file> [num_workers]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    
    if not Path(input_file).exists():
        print(f"Error: {input_file} not found")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("SUBPROCESS PARALLEL PROCESSING")
    print("="*70)
    print(f"Input file: {input_file}")
    print(f"Number of workers: {num_workers}")
    print("="*70 + "\n")
    
    # Prepare output directory
    output_dir = Path('data/processed/chunks')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Count and split
    print("Counting rows...")
    total_rows = count_data_rows(input_file)
    print(f"Total events: {total_rows:,}\n")
    
    chunks = split_into_chunks(total_rows, num_workers)
    print("Chunk distribution:")
    for i, (start, count) in enumerate(chunks):
        print(f"  Worker {i}: rows {start:,} to {start + count - 1:,} ({count:,} events)")
    print()
    
    # Launch all workers
    print("Launching workers...\n")
    overall_start = time.time()
    processes = []
    
    for i, (start, count) in enumerate(chunks):
        proc = launch_worker(input_file, start, count, i, output_dir)
        processes.append(proc)
    
    # Wait for all workers to complete
    print("\nWaiting for workers to complete...")
    for i, proc in enumerate(processes):
        proc.wait()
        if proc.returncode == 0:
            print(f"[Launcher] Worker {i} completed successfully")
        else:
            print(f"[Launcher] Worker {i} failed with code {proc.returncode}")
    
    overall_elapsed = time.time() - overall_start
    
    # Merge results
    print("\nMerging results...")
    final_results = merge_results(output_dir, num_workers)
    
    # Save final results
    final_output = Path('data/processed/subprocess_results.json')
    with open(final_output, 'w') as f:
        json.dump(final_results, f, indent=2)
    
    # Display results
    print("\n" + "="*70)
    print("PROCESSING COMPLETE")
    print("="*70)
    print(f"Total events: {final_results['total']:,}")
    print(f"Average energy: {final_results['avg_energy']:.2f}")
    print(f"High energy events: {final_results['high_energy_events']:,}")
    print("\nStatus distribution:")
    for status, count in sorted(final_results['status_counts'].items()):
        pct = 100 * count / final_results['total']
        print(f"  {status:>10}: {count:>7,} ({pct:>5.1f}%)")
    
    print(f"\n{'='*70}")
    print("PERFORMANCE")
    print("="*70)
    print(f"Wall clock time: {overall_elapsed:.2f}s")
    print(f"Total CPU time: {final_results['total_processing_time']:.2f}s")
    print(f"Speedup: {final_results['total_processing_time'] / overall_elapsed:.2f}x")
    print(f"Events/sec: {final_results['total'] / overall_elapsed:,.0f}")
    print(f"\nResults: {final_output}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()