#!/usr/bin/env python3
"""
compare_parallel.py - Compare sequential vs parallel processing performance
"""

import subprocess
import time
import sys
from pathlib import Path

def run_sequential(input_file):
    """Run sequential processing using original process_chunk.py"""
    print("\n" + "="*70)
    print("SEQUENTIAL PROCESSING")
    print("="*70)
    
    start_time = time.time()
    cmd = ['python3', 'process_chunk.py', input_file, '0', '100000']
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - start_time
    
    print(result.stdout)
    print(f"Time: {elapsed:.2f}s")
    return elapsed

def run_multiprocessing(input_file, num_workers):
    """Run multiprocessing parallel version"""
    print("\n" + "="*70)
    print(f"MULTIPROCESSING (parallel_processor.py) - {num_workers} workers")
    print("="*70)
    
    start_time = time.time()
    cmd = ['python3', 'parallel_processor.py', input_file, str(num_workers)]
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = time.time() - start_time
    
    return elapsed

def run_subprocess(input_file, num_workers):
    """Run subprocess parallel version"""
    print("\n" + "="*70)
    print(f"SUBPROCESS (subprocess_parallel.py) - {num_workers} workers")
    print("="*70)
    
    start_time = time.time()
    cmd = ['python3', 'subprocess_parallel.py', input_file, str(num_workers)]
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = time.time() - start_time
    
    return elapsed

def main():
    input_file = 'data/raw/events.csv'
    
    if not Path(input_file).exists():
        print(f"Error: {input_file} not found. Run generate_data.py first.")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("PARALLEL PROCESSING PERFORMANCE COMPARISON")
    print("="*70)
    print(f"Dataset: {input_file}")
    print("Testing sequential vs parallel approaches...\n")
    
    # Run tests
    results = {}
    
    # Sequential baseline
    results['sequential'] = run_sequential(input_file)
    
    # Multiprocessing with different worker counts
    for num_workers in [2, 4, 8]:
        key = f'multiprocessing_{num_workers}'
        results[key] = run_multiprocessing(input_file, num_workers)
    
    # Subprocess parallel
    results['subprocess_4'] = run_subprocess(input_file, 4)
    
    # Summary
    print("\n" + "="*70)
    print("PERFORMANCE SUMMARY")
    print("="*70)
    
    baseline = results['sequential']
    print(f"{'Method':<30} {'Time (s)':<12} {'Speedup':<10}")
    print("-" * 70)
    
    for method, elapsed in results.items():
        speedup = baseline / elapsed
        print(f"{method:<30} {elapsed:>10.2f}  {speedup:>9.2f}x")
    
    print("\n" + "="*70)
    print("RECOMMENDATIONS:")
    print("="*70)
    best = min(results.items(), key=lambda x: x[1])
    print(f"✓ Fastest method: {best[0]} ({best[1]:.2f}s)")
    print(f"✓ Speedup over sequential: {baseline / best[1]:.2f}x")
    print("\nFor production:")
    print("  - Use multiprocessing for CPU-bound tasks (better memory sharing)")
    print("  - Use subprocess for independent processes or mixed languages")
    print("  - Optimal workers: typically equal to CPU cores")
    print("="*70 + "\n")

if __name__ == '__main__':
    main()