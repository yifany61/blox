import queue
import time

class ClusterState:
    def __init__(self, cluster_definition):
        self.machines = cluster_definition
    
    def allocate_resources(self, job_resources):
        for machine in self.machines:
            if all(machine[res] >= job_resources[res] for res in job_resources):
                for res in job_resources:
                    machine[res] -= job_resources[res]
                return machine
        return None
    
    def free_resources(self, job_resources, machine):
        for res in job_resources:
            machine[res] += job_resources[res]

class FIFOScheduler:
    def __init__(self, cluster_definition):
        self.job_queue = queue.Queue()
        self.cluster_state = ClusterState(cluster_definition)
        self.job_times = {}
        self.last_job_end_time = 0
        self.completed = False
        self.total_jobs_added = 0
        self.total_jobs_completed = 0

    def add_job(self, job_id, job_info):
        submit_time = time.time()
        self.job_queue.put((job_id, job_info, submit_time))
        print(f'Job {job_id} submitted with resources {job_info["resources"]} at {submit_time}')

    def schedule_jobs(self):
        current_time = time.time()
        if current_time >= self.last_job_end_time and not self.job_queue.empty():
            job_id, job_info, submit_time = self.job_queue.get()
            allocated_machine = self.cluster_state.allocate_resources(job_info['resources'])
            if allocated_machine:
                start_time = max(self.last_job_end_time, current_time)
                end_time = start_time + job_info['duration']
                self.last_job_end_time = end_time
                self.job_times[job_id] = {'submit_time': submit_time, 'start_time': start_time, 'end_time': end_time, 'machine': allocated_machine}
                print(f'Job {job_id} started at {start_time} and will end at {end_time}')
                # Simulating job completion and resource deallocation
                self.cluster_state.free_resources(job_info['resources'], allocated_machine)
            else:
                print(f'Job {job_id} could not be scheduled due to insufficient resources.')
    
    # def check_completed_jobs(self):
    #     current_time = time.time()
    #     for job_id, job_info in list(self.job_times.items()):
    #         if current_time >= job_info['end_time']:
    #             yield job_id, job_info
    #             del self.job_times[job_id] 

    def check_completed_jobs(self):
        current_time = time.time()
        jobs_removed = 0
        for job_id, job_info in list(self.job_times.items()):
            if current_time >= job_info['end_time']:
                yield job_id, job_info
                del self.job_times[job_id]
                self.total_jobs_completed += 1
                jobs_removed += 1

        # Check if all known jobs are completed and the queue is empty
        if self.total_jobs_added == self.total_jobs_completed and self.job_queue.empty():
            self.completed = True

        return jobs_removed  

if __name__ == '__main__':
    cluster_state = ClusterState()
    scheduler = FIFOScheduler(cluster_state)
    # Simulate adding jobs to the scheduler with resource requirements
    scheduler.add_job(1, {'job_name': 'Job1', 'duration': 5, 'resources': {'cpus': 2, 'gpus': 1, 'memory': 4}})
    scheduler.add_job(2, {'job_name': 'Job2', 'duration': 3, 'resources': {'cpus': 1, 'gpus': 0, 'memory': 2}})
    start_time = time.time()
    while time.time() - start_time < 15:  # Simulate for 15 seconds
        scheduler.schedule_jobs()
        time.sleep(0.1) 
