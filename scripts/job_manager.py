#!/usr/bin/env python3
"""
job_manager.py - Manages job state and lifecycle
"""

import uuid
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum
import threading

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobManager:
    """In-memory job manager for tracking processing jobs"""
    
    def __init__(self):
        self.jobs: Dict[str, Dict] = {}
        self.lock = threading.Lock()
    
    def create_job(
        self,
        input_file: str,
        num_workers: int = 4,
        method: str = "multiprocessing"
    ) -> str:
        """Create a new job and return its ID"""
        job_id = str(uuid.uuid4())
        
        with self.lock:
            self.jobs[job_id] = {
                'id': job_id,
                'input_file': input_file,
                'num_workers': num_workers,
                'method': method,
                'status': JobStatus.PENDING,
                'progress': 0.0,
                'created_at': datetime.now().isoformat(),
                'started_at': None,
                'completed_at': None,
                'results': None,
                'error': None
            }
        
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job information by ID"""
        with self.lock:
            return self.jobs.get(job_id)
    
    def update_job_status(self, job_id: str, status: str):
        """Update job status"""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]['status'] = status
                
                if status == JobStatus.RUNNING and not self.jobs[job_id]['started_at']:
                    self.jobs[job_id]['started_at'] = datetime.now().isoformat()
                elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
    
    def update_progress(self, job_id: str, progress: float):
        """Update job progress (0.0 to 1.0)"""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]['progress'] = min(max(progress, 0.0), 1.0)
    
    def complete_job(self, job_id: str, results: Dict):
        """Mark job as completed with results"""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]['status'] = JobStatus.COMPLETED
                self.jobs[job_id]['results'] = results
                self.jobs[job_id]['progress'] = 1.0
                self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
    
    def fail_job(self, job_id: str, error: str):
        """Mark job as failed with error message"""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]['status'] = JobStatus.FAILED
                self.jobs[job_id]['error'] = error
                self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
    
    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """List all jobs, optionally filtered by status"""
        with self.lock:
            jobs = list(self.jobs.values())
            
            # Filter by status if provided
            if status:
                jobs = [j for j in jobs if j['status'] == status]
            
            # Sort by created_at (newest first)
            jobs.sort(key=lambda x: x['created_at'], reverse=True)
            
            # Apply limit
            if limit:
                jobs = jobs[:limit]
            
            return jobs
    
    def get_active_jobs(self) -> List[Dict]:
        """Get all jobs that are currently pending or running"""
        return self.list_jobs(status=JobStatus.RUNNING) + \
               self.list_jobs(status=JobStatus.PENDING)
    
    def clear_completed_jobs(self, older_than_hours: int = 24):
        """Clear completed/failed jobs older than specified hours"""
        with self.lock:
            cutoff_time = datetime.now().timestamp() - (older_than_hours * 3600)
            
            jobs_to_remove = []
            for job_id, job in self.jobs.items():
                if job['status'] in [JobStatus.COMPLETED, JobStatus.FAILED]:
                    completed_time = datetime.fromisoformat(job['completed_at']).timestamp()
                    if completed_time < cutoff_time:
                        jobs_to_remove.append(job_id)
            
            for job_id in jobs_to_remove:
                del self.jobs[job_id]
            
            return len(jobs_to_remove)