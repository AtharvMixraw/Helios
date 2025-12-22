#!/usr/bin/env python3
"""
test_api.py - Comprehensive API testing suite
"""

import requests
import time
import sys
from pathlib import Path

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_test(name):
    print(f"\n{Colors.BLUE}Testing: {name}{Colors.END}")

def print_pass(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")

def print_fail(msg):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")

def print_info(msg):
    print(f"{Colors.YELLOW}ℹ {msg}{Colors.END}")

class APITester:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.passed = 0
        self.failed = 0
    
    def test_connection(self):
        """Test if API is reachable"""
        print_test("API Connection")
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            if response.status_code == 200:
                print_pass(f"API is reachable at {self.base_url}")
                self.passed += 1
                return True
            else:
                print_fail(f"API returned status {response.status_code}")
                self.failed += 1
                return False
        except Exception as e:
            print_fail(f"Cannot connect to API: {e}")
            print_info("Make sure the server is running: python3 main.py")
            self.failed += 1
            return False
    
    def test_health_endpoint(self):
        """Test health check endpoint"""
        print_test("Health Check Endpoint")
        try:
            response = requests.get(f"{self.base_url}/health")
            response.raise_for_status()
            data = response.json()
            
            assert 'status' in data, "Missing 'status' field"
            assert data['status'] == 'healthy', "Status is not healthy"
            assert 'timestamp' in data, "Missing 'timestamp' field"
            
            print_pass("Health endpoint working correctly")
            print_info(f"Active jobs: {data.get('active_jobs', 0)}")
            self.passed += 1
            return True
        except Exception as e:
            print_fail(f"Health check failed: {e}")
            self.failed += 1
            return False
    
    def test_job_submission(self):
        """Test job submission"""
        print_test("Job Submission")
        
        # Check if data file exists
        if not Path("data/raw/events.csv").exists():
            print_fail("data/raw/events.csv not found")
            print_info("Run: python3 generate_data.py")
            self.failed += 1
            return None
        
        try:
            payload = {
                "input_file": "data/raw/events.csv",
                "num_workers": 2,
                "method": "multiprocessing"
            }
            response = requests.post(f"{self.base_url}/jobs/submit", json=payload)
            response.raise_for_status()
            data = response.json()
            
            assert 'job_id' in data, "Missing job_id"
            assert 'status' in data, "Missing status"
            
            job_id = data['job_id']
            print_pass(f"Job submitted successfully")
            print_info(f"Job ID: {job_id[:16]}...")
            self.passed += 1
            return job_id
        except Exception as e:
            print_fail(f"Job submission failed: {e}")
            self.failed += 1
            return None
    
    def test_job_status(self, job_id):
        """Test job status retrieval"""
        print_test("Job Status Retrieval")
        
        if not job_id:
            print_info("Skipping (no job ID)")
            return False
        
        try:
            response = requests.get(f"{self.base_url}/jobs/{job_id}/status")
            response.raise_for_status()
            data = response.json()
            
            assert 'job_id' in data, "Missing job_id"
            assert 'status' in data, "Missing status"
            assert data['job_id'] == job_id, "Job ID mismatch"
            
            print_pass("Status endpoint working")
            print_info(f"Job status: {data['status']}")
            if data.get('progress'):
                print_info(f"Progress: {data['progress']*100:.1f}%")
            self.passed += 1
            return True
        except Exception as e:
            print_fail(f"Status check failed: {e}")
            self.failed += 1
            return False
    
    def test_job_completion(self, job_id):
        """Test waiting for job completion"""
        print_test("Job Completion")
        
        if not job_id:
            print_info("Skipping (no job ID)")
            return False
        
        try:
            print_info("Waiting for job to complete (max 60s)...")
            start_time = time.time()
            
            while time.time() - start_time < 60:
                response = requests.get(f"{self.base_url}/jobs/{job_id}/status")
                response.raise_for_status()
                status_data = response.json()
                
                status = status_data['status']
                progress = status_data.get('progress', 0)
                
                print(f"  Status: {status} ({progress*100:.0f}%)   ", end='\r')
                
                if status == 'completed':
                    print()  # New line
                    print_pass("Job completed successfully")
                    self.passed += 1
                    return True
                elif status == 'failed':
                    print()  # New line
                    error = status_data.get('error', 'Unknown error')
                    print_fail(f"Job failed: {error}")
                    self.failed += 1
                    return False
                
                time.sleep(1)
            
            print()  # New line
            print_fail("Job did not complete within 60s")
            self.failed += 1
            return False
            
        except Exception as e:
            print_fail(f"Error waiting for job: {e}")
            self.failed += 1
            return False
    
    def test_job_results(self, job_id):
        """Test result retrieval"""
        print_test("Job Result Retrieval")
        
        if not job_id:
            print_info("Skipping (no job ID)")
            return False
        
        try:
            response = requests.get(f"{self.base_url}/jobs/{job_id}/result")
            response.raise_for_status()
            data = response.json()
            
            assert 'job_id' in data, "Missing job_id"
            assert 'status' in data, "Missing status"
            assert 'results' in data, "Missing results"
            
            results = data['results']
            assert 'total' in results, "Missing total in results"
            
            print_pass("Results retrieved successfully")
            print_info(f"Total events: {results['total']:,}")
            print_info(f"Average energy: {results.get('avg_energy', 0):.2f}")
            self.passed += 1
            return True
            
        except Exception as e:
            print_fail(f"Result retrieval failed: {e}")
            self.failed += 1
            return False
    
    def test_list_jobs(self):
        """Test listing jobs"""
        print_test("List Jobs Endpoint")
        
        try:
            response = requests.get(f"{self.base_url}/jobs")
            response.raise_for_status()
            jobs = response.json()
            
            assert isinstance(jobs, list), "Jobs should be a list"
            
            print_pass(f"Listed {len(jobs)} jobs")
            if jobs:
                statuses = {}
                for job in jobs:
                    status = job['status']
                    statuses[status] = statuses.get(status, 0) + 1
                print_info(f"Status breakdown: {statuses}")
            self.passed += 1
            return True
            
        except Exception as e:
            print_fail(f"List jobs failed: {e}")
            self.failed += 1
            return False
    
    def test_stats_endpoint(self):
        """Test statistics endpoint"""
        print_test("Statistics Endpoint")
        
        try:
            response = requests.get(f"{self.base_url}/stats")
            response.raise_for_status()
            stats = response.json()
            
            assert 'total_jobs' in stats, "Missing total_jobs"
            assert 'by_status' in stats, "Missing by_status"
            
            print_pass("Statistics retrieved")
            print_info(f"Total jobs: {stats['total_jobs']}")
            print_info(f"Active jobs: {stats.get('active_jobs', 0)}")
            self.passed += 1
            return True
            
        except Exception as e:
            print_fail(f"Stats endpoint failed: {e}")
            self.failed += 1
            return False
    
    def test_invalid_job_id(self):
        """Test handling of invalid job ID"""
        print_test("Invalid Job ID Handling")
        
        try:
            fake_job_id = "00000000-0000-0000-0000-000000000000"
            response = requests.get(f"{self.base_url}/jobs/{fake_job_id}/status")
            
            if response.status_code == 404:
                print_pass("Correctly returns 404 for invalid job ID")
                self.passed += 1
                return True
            else:
                print_fail(f"Expected 404, got {response.status_code}")
                self.failed += 1
                return False
                
        except Exception as e:
            print_fail(f"Error testing invalid job: {e}")
            self.failed += 1
            return False
    
    def run_all_tests(self):
        """Run complete test suite"""
        print("="*70)
        print("API Test Suite")
        print("="*70)
        
        # Test 1: Connection
        if not self.test_connection():
            print("\n" + "="*70)
            print(f"{Colors.RED}Cannot proceed without API connection{Colors.END}")
            print("="*70)
            return False
        
        # Test 2: Health
        self.test_health_endpoint()
        
        # Test 3: Submit job
        job_id = self.test_job_submission()
        
        # Test 4: Check status
        self.test_job_status(job_id)
        
        # Test 5: Wait for completion
        completed = self.test_job_completion(job_id)
        
        # Test 6: Get results (only if completed)
        if completed:
            self.test_job_results(job_id)
        
        # Test 7: List jobs
        self.test_list_jobs()
        
        # Test 8: Stats
        self.test_stats_endpoint()
        
        # Test 9: Error handling
        self.test_invalid_job_id()
        
        # Summary
        print("\n" + "="*70)
        print("Test Summary")
        print("="*70)
        total = self.passed + self.failed
        print(f"Total tests: {total}")
        print(f"{Colors.GREEN}Passed: {self.passed}{Colors.END}")
        print(f"{Colors.RED}Failed: {self.failed}{Colors.END}")
        
        if self.failed == 0:
            print(f"\n{Colors.GREEN}🎉 All tests passed!{Colors.END}")
        else:
            print(f"\n{Colors.YELLOW}⚠ Some tests failed{Colors.END}")
        
        print("="*70 + "\n")
        
        return self.failed == 0

def main():
    tester = APITester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()