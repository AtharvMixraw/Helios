#!/usr/bin/env python3
"""
main.py - FastAPI Backend for Distributed Event Processing
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, List
import uuid
from datetime import datetime
from enum import Enum
import asyncio

from job_manager import JobManager, JobStatus
from compute import ComputeEngine

# Initialize FastAPI app
app = FastAPI(
    title="Distributed Event Processing API",
    description="Backend API for parallel event processing",
    version="1.0.0"
)

# Initialize job manager
job_manager = JobManager()
compute_engine = ComputeEngine()

# Request/Response Models
class JobSubmitRequest(BaseModel):
    input_file: str
    num_workers: Optional[int] = 4
    method: Optional[str] = "multiprocessing"  # or "subprocess"
    
    class Config:
        json_schema_extra = {
            "example": {
                "input_file": "data/raw/events.csv",
                "num_workers": 4,
                "method": "multiprocessing"
            }
        }

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str

class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    progress: Optional[float] = None
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None

class JobResultResponse(BaseModel):
    job_id: str
    status: str
    results: Optional[Dict] = None
    error: Optional[str] = None

# API Endpoints
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Distributed Event Processing API",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_jobs": len(job_manager.get_active_jobs()),
        "total_jobs": len(job_manager.jobs)
    }

@app.post("/jobs/submit", response_model=JobResponse)
async def submit_job(
    request: JobSubmitRequest, 
    background_tasks: BackgroundTasks
):
    """
    Submit a new processing job
    
    - **input_file**: Path to the CSV file to process
    - **num_workers**: Number of parallel workers (default: 4)
    - **method**: Processing method - 'multiprocessing' or 'subprocess' (default: multiprocessing)
    """
    try:
        # Create job
        job_id = job_manager.create_job(
            input_file=request.input_file,
            num_workers=request.num_workers,
            method=request.method
        )
        
        # Schedule job execution in background
        background_tasks.add_task(
            execute_job,
            job_id=job_id,
            input_file=request.input_file,
            num_workers=request.num_workers,
            method=request.method
        )
        
        return JobResponse(
            job_id=job_id,
            status="submitted",
            message=f"Job {job_id} submitted successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status of a specific job
    
    - **job_id**: The unique job identifier
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    return JobStatusResponse(
        job_id=job['id'],
        status=job['status'],
        progress=job.get('progress'),
        created_at=job['created_at'],
        started_at=job.get('started_at'),
        completed_at=job.get('completed_at'),
        error=job.get('error')
    )

@app.get("/jobs/{job_id}/result", response_model=JobResultResponse)
async def get_job_result(job_id: str):
    """
    Get the results of a completed job
    
    - **job_id**: The unique job identifier
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    if job['status'] not in ['completed', 'failed']:
        raise HTTPException(
            status_code=400, 
            detail=f"Job {job_id} is still {job['status']}"
        )
    
    return JobResultResponse(
        job_id=job['id'],
        status=job['status'],
        results=job.get('results'),
        error=job.get('error')
    )

@app.get("/jobs", response_model=List[JobStatusResponse])
async def list_jobs(
    status: Optional[str] = None,
    limit: Optional[int] = 100
):
    """
    List all jobs, optionally filtered by status
    
    - **status**: Filter by job status (pending, running, completed, failed)
    - **limit**: Maximum number of jobs to return (default: 100)
    """
    jobs = job_manager.list_jobs(status=status, limit=limit)
    
    return [
        JobStatusResponse(
            job_id=job['id'],
            status=job['status'],
            progress=job.get('progress'),
            created_at=job['created_at'],
            started_at=job.get('started_at'),
            completed_at=job.get('completed_at'),
            error=job.get('error')
        )
        for job in jobs
    ]

@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """
    Cancel a running job
    
    - **job_id**: The unique job identifier
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    if job['status'] not in ['pending', 'running']:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status {job['status']}"
        )
    
    job_manager.update_job_status(job_id, 'cancelled')
    
    return {
        "job_id": job_id,
        "status": "cancelled",
        "message": f"Job {job_id} cancelled successfully"
    }

@app.get("/stats")
async def get_stats():
    """Get overall system statistics"""
    all_jobs = job_manager.list_jobs()
    
    stats = {
        "total_jobs": len(all_jobs),
        "by_status": {},
        "active_jobs": len(job_manager.get_active_jobs())
    }
    
    # Count by status
    for job in all_jobs:
        status = job['status']
        stats['by_status'][status] = stats['by_status'].get(status, 0) + 1
    
    return stats

# Background task executor
async def execute_job(
    job_id: str,
    input_file: str,
    num_workers: int,
    method: str
):
    """Execute the job in the background"""
    try:
        # Update status to running
        job_manager.update_job_status(job_id, 'running')
        
        # Execute the compute task
        results = await compute_engine.process_events(
            input_file=input_file,
            num_workers=num_workers,
            method=method,
            progress_callback=lambda p: job_manager.update_progress(job_id, p)
        )
        
        # Store results and mark as completed
        job_manager.complete_job(job_id, results)
        
    except Exception as e:
        # Mark job as failed with error
        job_manager.fail_job(job_id, str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )