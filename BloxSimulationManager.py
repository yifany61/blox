import os
import sys
import json
import grpc
import argparse
from fifo_scheduler import FIFOScheduler
import numpy as np
from workload import Workload
# from workload import Workload
from concurrent import futures
from typing import Tuple

# import matplotlib
# import matplotlib.pyplot as plt
# import matplotlib.lines as lines
from collections import defaultdict

# something in pylab * screws up with random library, for now just overwriting
import random

# sys.path.append(os.path.join(os.path.dirname(__file__), "./deployment/grpc_stubs"))
sys.path.append('E:/shivaram/blox/blox/deployment/grpc_proto')
from blox.deployment.grpc_proto import rm_pb2
from blox.deployment.grpc_proto import rm_pb2_grpc
from blox.deployment.grpc_proto import simulator_pb2
from blox.deployment.grpc_proto import simulator_pb2_grpc
import traceback

class SimulationManager(simulator_pb2_grpc.SimServerServicer):
    def __init__(self, cluster_definition, simulation_parameters, job_mgr="localhost:8888"):
        self.cluster_definition = cluster_definition
        self.simulation_parameters = simulation_parameters
        self.jobs_to_track = simulation_parameters['jobs_to_track']
        self.simulation_file_prefix = simulation_parameters['simulation_file_prefix']
        self.current_job_config = None
        self.ipaddr_rm = job_mgr

        self.setup_cluster()

        self.simulator_config = []
        self._generate_simulator_configs()
        self.prev_job_time = 0
        self.latest_job = None
        self.prev_job = None
        
        # Set up grpc server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        simulator_pb2_grpc.add_SimServerServicer_to_server(self, self.server)
        self.server.add_insecure_port('[::]:50051')
        self.server.start()

    def setup_cluster(self):
        for node in self.cluster_definition:
            request_to_rm = rm_pb2.RegisterRequest()
            request_to_rm.ipaddr = node.get('ipaddr', "")
            request_to_rm.numGPUs = node.get('num_GPUs') # necessary
            # provided or default (not sure the true default should be ...)
            request_to_rm.gpuUUIDs = node.get('GPU_UUID', "temp")
            request_to_rm.memoryCapacity = node.get('memory_capacity', 16)
            request_to_rm.numCPUcores = node.get('num_CPUs', 2)
            request_to_rm.numaAvailable = node.get('is_numa_available', False)
            request_to_rm.cpuMaping[0] = 0

            with grpc.insecure_channel(self.ipaddr_rm) as channel:
                stub = rm_pb2_grpc.RMServerStub(channel)
                response = stub.RegisterWorker(request_to_rm)
        print(f"Number of machines sent {len(self.cluster_definition)}.")

    def _generate_simulator_configs(self):
        self.schedulers = self.simulation_parameters.get('schedulers', ['Fifo'])
        self.placement_policies = self.simulation_parameters.get('placement_policies', ['Place'])
        self.acceptance_policies = self.simulation_parameters.get('acceptance_policies', ['AcceptAll'])
        list_jobs_per_hour = self.simulation_parameters.get('list_jobs_per_hour', [10, 40])
        
        for scheduler in self.schedulers:
            for placement_policy in self.placement_policies:
                for acceptance_policy in self.acceptance_policies:
                    for load in list_jobs_per_hour:
                        self.simulator_config.append({
                            "scheduler": scheduler,
                            "load": load,
                            "start_id_track": self.jobs_to_track[0],
                            "stop_id_track": self.jobs_to_track[1],
                            "placement_policy": placement_policy,
                            "acceptance_policy": acceptance_policy,
                        })
        
        # # add an empty config to know when to terminate
        # self.simulator_config.append({
        #     "scheduler": "",
        #     "load": -1,
        #     "start_id_track": 0,
        #     "stop_id_track": 0,
        #     "placement_policy": placement_policy,
        #     "acceptance_policy": acceptance_policy,
        # })

    def get_new_sim_config(self, request, context):
        """
        Provide new_job config to run the simulator.
        """
        try:
            self.current_job_config = self.simulator_config.pop(0)
            self.scheduler.add_job(self.current_job_config)
            self.workload = self._generate_workload(self.current_job_config)

            job_config_send = rm_pb2.JsonResponse()
            job_config_send.response = json.dumps(self.current_job_config)
            self.setup_cluster()

            # reseting the job time
            self.prev_job_time = 0
            self.prev_job = None

            print("Job config {}".format(self.current_job_config))
            return job_config_send
        except IndexError:
            # no job configuration are left
            print("No more job configurations available.")

            termination_config = {
                "scheduler": "",
                "load": -1,
                "start_id_track": 0,
                "stop_id_track": 0,
            }
            job_config_send = rm_pb2.JsonResponse()
            job_config_send.response = json.dumps(termination_config)
            return job_config_send

    def GetJobs(self, request, context) -> rm_pb2.JsonResponse:
        """
        Return a dictionary of jobs for simulating.
        """
        simulator_time = request.value 
        job_to_run_dict = dict()
        jcounter = 0
        print("Simulator time {}".format(simulator_time))
        new_job = None
        
        try:
            while True:
                if self.prev_job is None:
                    new_job = self.workload.generate_next_job(self.prev_job_time)
                    new_job_dict = self._clean_sim_job(new_job.__dict__)
                else:
                    print("Self previous job")
                    new_job_dict = self.prev_job

                print("New job dict arrival time {}".format(
                        new_job_dict["job_arrival_time"]
                    ))

                if new_job_dict:
                    if new_job_dict["job_arrival_time"] <= simulator_time:
                        print("In getting more jobs")
                        job_to_run_dict[jcounter] = new_job_dict
                        self.prev_job_time = new_job_dict["job_arrival_time"]
                        self.prev_job = None 
                        jcounter += 1
                    else:
                        # the next job's arrival time is beyond the current simulation time
                        print("return previous job")
                        self.prev_job = new_job_dict
                        self.prev_job_time = new_job_dict["job_arrival_time"]
                        break
                
            return rm_pb2.JsonResponse(response=json.dumps(job_to_run_dict))
        except Exception as e:
            print("Exception occurred while getting jobs: {}".format(e))
            traceback.print_exc()
            return rm_pb2.JsonResponse(response=json.dumps({"error": "Failed to generate jobs"}))

    def _clean_sim_job(self, new_job: dict) -> dict:
        """
        Preprocesses the job for simulations.
        Cleans some fields and non serializable input
        """
        # new_job_time = random.randint(36000, 86400)
        # new_job["job_total_iteration"] = new_job_time
        # new_job["job_duration"] = new_job_time
        new_job["simulation"] = True
        new_job["submit_time"] = new_job["job_arrival_time"]
        # temporary fix not sure why this is happening though
        if "logger" in new_job:
            new_job.pop("logger")
        if "job_task" in new_job:
            new_job.pop("job_task")
        if "job_model" in new_job:
            new_job.pop("job_model")

        new_job["num_GPUs"] = new_job["job_gpu_demand"]
        # new_job["params_to_track"] = [
        # "per_iter_time",
        # "attained_service",
        # ]
        # new_job["default_values"] = [0, 0]
        return new_job

    def NotifyCompletion(self, request, context):
        job_id = request.value
        print(f"Job {job_id} completed.")
        return rm_pb2.JsonResponse(response=json.dumps({"message": "Job completion acknowledged"}))

    def stop(self):
        self.server.stop(0)

    def _generate_workload(self, workload_config):
        """
        Generate workload for a given config
        """
        # set the random seed before generating the workload
        random.seed(self.random_seed)
        # print("After random seed")
        return Workload(
            self.cluster_job_log,
            jobs_per_hour=workload_config["load"],
            exponential=self.exponential,
            multigpu=self.multigpu,
            small_trace=self.small_trace,
            series_id_filter=self.job_ids_to_track,
            model_class_split=self.model_class_split,
            # TODO: Fix this
            per_server_size=[
                self.gpus_per_machine,
                self.num_cpu_cores,
                self.memory_per_machine,
                500,
                40,
            ],
            num_jobs_default=self.num_jobs_default,
        )

    def run_simulation(self, workload):
        """
        Simulates job processing based on the given workload until all jobs specified in jobs_to_track are completed.
        """
        while True:
            config_response = self.get_new_sim_config(None, None)
            config = json.loads(config_response.response)

            if config["scheduler"] == "":
                print("All specified jobs have been processed. Simulation complete.")
                break;


if __name__ == "__main__":
    cluster_definition = [
        {'num_CPUs': 2, 'num_GPUs': 4, 'GPU_UUID': 'temp', 'GPU_Type': 'V100'},
        {'num_CPUs': 2, 'num_GPUs': 4, 'GPU_UUID': 'temp', 'GPU_Type': 'V100'}
    ]
    simulation_parameters = {
        'jobs_to_track': [10, 40], 
        'simulation_file_prefix': "/scratch/blox/test_simulation",
        'schedulers': ['Fifo'],
        'placement_policy': ['Place'],
        'acceptance_policy': ['AcceptAll'],
        # 'list_jobs_per_hour': [10, 40]
    }
    sim_manager = SimulationManager(cluster_definition, simulation_parameters, job_mgr="localhost:8888")
    workload = Workload("trace_file_name", 1.0)  # workload_definition = Workload(trace_file_name, load_avg, **kwargs) 

    try:
        sim_manager.run_simulation(workload)
    except KeyboardInterrupt:
        sim_manager.stop()
        print("Simulation stopped.")
