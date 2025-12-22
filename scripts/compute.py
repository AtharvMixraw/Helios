#!/usr/bin/env python3
"""
compute.py - Core computation engine (refactored from parallel_processor.py)
"""

import csv
import time
from pathlib import Path
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from typing import Dict, Optional, Callable
import asyncio
import subprocess
import json

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

def process_chunk_worker(args):
    """Worker function for multiprocessing"""
    file_path, start_row, num_rows, chunk_id = args
    
    processor = EventProcessor()
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            if i >= start_row + num_rows:
                break
            processor.process_event(row)
    
    results = processor.get_results()
    results['chunk_id'] = chunk_id
    
    return results

def merge_results(partial_results):
    """Merge results from all workers"""
    merged = {
        'total': 0,
        'status_counts': defaultdict(int),
        'sensor_counts': defaultdict(int),
        'energy_sum': 0.0,
        'high_energy_events': 0,
        'chunks_processed': len(partial_results)
    }
    
    for result in partial_results:
        merged['total'] += result['total']
        merged['energy_sum'] += result['energy_sum']
        merged['high_energy_events'] += result['high_energy_events']
        
        for status, count in result['status_counts'].items():
            merged['status_counts'][status] += count
        
        for sensor_id, count in result['sensor_counts'].items():
            merged['sensor_counts'][sensor_id] += count
    
    merged['avg_energy'] = merged['energy_sum'] / merged['total'] if merged['total'] > 0 else 0
    merged['status_counts'] = dict(merged['status_counts'])
    merged['sensor_counts'] = dict(merged['sensor_counts'])
    
    return merged

def count_data_rows(file_path: str) -> int:
    """Count total data rows in CSV"""
    with open(file_path, 'r') as f:
        return sum(1 for _ in f) - 1

def split_into_chunks(total_rows: int, num_workers: int):
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

class ComputeEngine:
    """Main compute engine that orchestrates parallel processing"""
    
    async def process_events(
        self,
        input_file: str,
        num_workers: int = 4,
        method: str = "multiprocessing",
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> Dict:
        """
        Process events using parallel workers
        
        Args:
            input_file: Path to input CSV file
            num_workers: Number of parallel workers
            method: Processing method ('multiprocessing' or 'subprocess')
            progress_callback: Optional callback for progress updates
        
        Returns:
            Dictionary containing processing results
        """
        if not Path(input_file).exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Count rows and split into chunks
        total_rows = count_data_rows(input_file)
        chunks = split_into_chunks(total_rows, num_workers)
        
        # Update progress
        if progress_callback:
            progress_callback(0.1)
        
        # Execute based on method
        if method == "multiprocessing":
            results = await self._process_multiprocessing(
                input_file, chunks, num_workers, progress_callback
            )
        elif method == "subprocess":
            results = await self._process_subprocess(
                input_file, chunks, num_workers, progress_callback
            )
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Final progress update
        if progress_callback:
            progress_callback(1.0)
        
        return results
    
    async def _process_multiprocessing(
        self,
        input_file: str,
        chunks: list,
        num_workers: int,
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> Dict:
        """Process using multiprocessing.Pool"""
        
        # Prepare worker arguments
        worker_args = [
            (input_file, start, count, i)
            for i, (start, count) in enumerate(chunks)
        ]
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        
        def run_pool():
            with Pool(processes=num_workers) as pool:
                return pool.map(process_chunk_worker, worker_args)
        
        # Update progress during processing
        if progress_callback:
            progress_callback(0.3)
        
        partial_results = await loop.run_in_executor(None, run_pool)
        
        if progress_callback:
            progress_callback(0.8)
        
        # Merge results
        final_results = merge_results(partial_results)
        final_results['method'] = 'multiprocessing'
        final_results['num_workers'] = num_workers
        
        return final_results
    
    async def _process_subprocess(
        self,
        input_file: str,
        chunks: list,
        num_workers: int,
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> Dict:
        """Process using subprocess workers"""
        
        # Prepare output directory
        output_dir = Path('data/processed/chunks')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Launch all workers
        processes = []
        for i, (start, count) in enumerate(chunks):
            output_file = output_dir / f'chunk_{i}.json'
            cmd = [
                'python3', 'worker.py',
                input_file, str(start), str(count), str(i), str(output_file)
            ]
            proc = subprocess.Popen(cmd)
            processes.append((proc, output_file))
        
        if progress_callback:
            progress_callback(0.3)
        
        # Wait for all processes
        for proc, _ in processes:
            proc.wait()
        
        if progress_callback:
            progress_callback(0.7)
        
        # Collect results
        partial_results = []
        for _, output_file in processes:
            if output_file.exists():
                with open(output_file, 'r') as f:
                    partial_results.append(json.load(f))
        
        if progress_callback:
            progress_callback(0.9)
        
        # Merge results
        final_results = merge_results(partial_results)
        final_results['method'] = 'subprocess'
        final_results['num_workers'] = num_workers
        
        return final_results