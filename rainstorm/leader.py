import asyncio
import grpc
import os
import shutil
import threading
import signal
import logging
import subprocess
from collections import defaultdict
import json


import rainstorm_pb2
import rainstorm_pb2_grpc

from hyDFS import RingNode

import heapq


class Leader(rainstorm_pb2_grpc.RainStormServicer):
    def __init__(self, base_dir="temp_files"):

        # start serving 
        self.server = None
        
        # temp file to store the operations
        self.base_dir = base_dir
        
        # If the directory exists, clear it
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)

        # Recreate the directory
        os.makedirs(base_dir, exist_ok=True)

        # hyDFS node instance
        self.hydfs_node = RingNode()

        # Start other hyDFS nodes
        self.start_worker_hydfs_nodes()

        # handle shutdown
        self.shutdown_flag = threading.Event()
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        self.stop_event = asyncio.Event()

        # Logging
        self.log_file = '/home/chaskar2/distributed-logger/rainstorm/logs/leader.logs'
        self.init_logging()

        # Given list of worker nodes with their load (second value in the tuple)
        self.worker_load = [
            (2,'fa24-cs425-6901.cs.illinois.edu'),
            (0,'fa24-cs425-6902.cs.illinois.edu'),
            (0,'fa24-cs425-6903.cs.illinois.edu'),
            (0,'fa24-cs425-6904.cs.illinois.edu'),
            (0,'fa24-cs425-6905.cs.illinois.edu'),
            (3,'fa24-cs425-6906.cs.illinois.edu'),
            (0,'fa24-cs425-6907.cs.illinois.edu'),
            (0,'fa24-cs425-6908.cs.illinois.edu'),
            (0,'fa24-cs425-6909.cs.illinois.edu'),
            (5,'fa24-cs425-6910.cs.illinois.edu')
        ]
        # Heapify the list
        heapq.heapify(self.worker_load)

        # num of tasks per stage
        self.num_tasks = 0

    async def serve(self):
        max_message_length = 128 * 1024 * 1024  # Set max message size to 128 MB
        self.server = grpc.aio.server(
            options=[
                ('grpc.max_receive_message_length', max_message_length),
                ('grpc.max_send_message_length', max_message_length),
            ]
        )

        rainstorm_pb2_grpc.add_RainStormServicer_to_server(self, self.server)
        self.server.add_insecure_port('fa24-cs425-6901.cs.illinois.edu:50051')
        await self.server.start()

        # Start monitoring the threading.Event
        asyncio.create_task(self.monitor_shutdown_flag())

        # Wait for the asyncio.Event
        await self.stop_event.wait()

        print("Stopping gRPC server...")
        await self.server.stop(grace=5)  # Gracefully shut down with a 5-second grace period
        print("gRPC server stopped.")

    
    async def create_task_mapping(self, op_exes, num_tasks):
        task_mapping = defaultdict(dict)
        
        # Appending source, special task

        print(f"Printing op_exes {op_exes}")

        op_exes.append("source")

        # Dictionary to keep track of the next available port for each server
        server_ports = defaultdict(lambda: 5002)

        # Loop over each op_exe to assign servers
        for idx, op in enumerate(op_exes):
            assigned_servers = {}

            # Assign `num_tasks` servers for each op_exe
            for i in range(num_tasks):
                # Extract the server with the minimum load
                min_server = heapq.heappop(self.worker_load)
                load, server_ip = min_server

                # Check if the server is in the membership list
                server_found = False
                for node in self.hydfs_node.process.membership_list:
                    if node['node_id'].startswith(server_ip):  # Check if the IP matches
                        server_found = True
                        break
                
                # If the server is in the membership list, increment its load and push back into the heap
                if server_found:
                    # Assign the next available port for the server
                    assigned_port = server_ports[server_ip]

                    # Increment the next available port for the server
                    server_ports[server_ip] += 1

                    # Assign the server IP and port as a single string
                    assigned_servers[i] = f"{server_ip}:{assigned_port}"

                    # Update the load and push back into the heap
                    new_load = load + 1
                    heapq.heappush(self.worker_load, (new_load, server_ip))
                else:
                    # If the server is not found, push it back with the same load
                    heapq.heappush(self.worker_load, min_server)
                    # raise ValueError(f"Server {server_ip} not found in membership list")

            # Add the assigned servers for the op_exe
            task_mapping[op] = assigned_servers

        return task_mapping


    def log(self, message):
        self.logger.info(message)
        print(message)

    def init_logging(self):
        # Create a specific logger for RingNode
        self.logger = logging.getLogger('RainstormLeaderLogger')
        self.logger.setLevel(logging.INFO)

        # Ensure only one handler is added to prevent duplicate logs
        if not self.logger.hasHandlers():
            # Set up file handler
            file_handler = logging.FileHandler(self.log_file, mode='w')
            file_handler.setLevel(logging.INFO)

            # Set up formatter and add it to handler
            formatter = logging.Formatter('%(asctime)s - %(message)s')
            file_handler.setFormatter(formatter)

            # Add handler to logger
            self.logger.addHandler(file_handler)

        self.log("Logging initialized for Rainstorm worker")

    
    def extract_servers_and_ports_from_mapping(self, task_mapping):
        
        server_list = []
        port_list = []

        for task, servers in task_mapping.items():
            print(f"Task: {task}")
            for task_id, server in servers.items():
                hostname, port = server.split(":") 
        
                server_list.append(hostname)
                port_list.append(port)

        return server_list, port_list

    async def SubmitJob(self, request, context):
        self.log(request.op_exe_names)

        src_file = request.hydfs_src_file
        dest_file = request.hydfs_dest_filename

        # Save and set permissions for the op_exes
        for i, binary in enumerate(request.op_exes):
            op_path = os.path.join(self.base_dir, f"{request.op_exe_names[i]}")
            print("Here")
            with open(op_path, "wb") as f:
                f.write(binary)
            os.chmod(op_path, 0o777)
        

        task_mapping = await self.create_task_mapping(request.op_exe_names, request.num_tasks)

        # Copy op exes to all the workers asynchronously
        await self.copy_exes_to_workers()

        # Get servers and ports to start the workers
        servers, ports = self.extract_servers_and_ports_from_mapping(task_mapping)

        # Start worker processes asynchronously
        await self.start_rainstorm_workers(task_mapping, src_file, dest_file, ports, servers)

        return rainstorm_pb2.JobResponse(message="Job submitted successfully!")


    def shutdown(self, signum, frame):
        """ Graceful shutdown function """
        self.log("Shutting down Rainstorm Leader ...")
        self.shutdown_flag.set()  # Signal the listener thread to stop
        self.hydfs_node.shutdown(signum, frame)

        self.log("Rainstorm Leader shut down gracefully.")
        self.stop_event.set()

    async def monitor_shutdown_flag(self):
        """Monitor threading.Event and set asyncio.Event when triggered."""
        print("Monitoring threading.Event for shutdown...")
        await asyncio.to_thread(self.shutdown_flag.wait)  # Wait for threading.Event
        print("threading.Event triggered. Setting asyncio.Event...")
        self.stop_event.set()
        

    def start_worker_hydfs_nodes(self):
        """Runs the hydfs_run_all_worker.sh script."""
        script_path = './scripts/hydfs_run_all_worker.sh'

        try:
            print(f"Running script: {script_path}")
            result = subprocess.run([script_path], check=True, text=True, capture_output=True)
            print("Script output:", result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Script failed with error: {e}")
            print("Error output:", e.stderr)
        except FileNotFoundError:
            print(f"The script {script_path} was not found.")



    async def start_rainstorm_workers(self, task_mapping, src_file, dest_file, ports, servers):
        """Starts the rainstorm workers by running the start_rainstorm_workers.sh script asynchronously using create_subprocess_exec."""
        # Convert mapping dictionary to JSON string
        mapping_dict_json = json.dumps(task_mapping, ensure_ascii=True)
        mapping_dict_json = mapping_dict_json.replace('"', '\\"')  # Escape quotes for shell
        
        print(f"Generated mapping dict: {mapping_dict_json}")

        # Convert list of ports to a comma-separated string
        ports_str = ",".join(map(str, ports))

        # Command to execute the shell script
        command = [
            "/home/chaskar2/distributed-logger/rainstorm/scripts/start_rainstorm_workers.sh",
            mapping_dict_json,
            src_file,
            dest_file,
            ports_str,
            *servers  # Unpack the server list
        ]

        try:
            # Run the script using create_subprocess_exec for non-blocking execution
            print(f"Running script: {command}")
            process = await asyncio.create_subprocess_exec(
                *command, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()  # Capture output and error

            if stdout:
                print(f"Script output: {stdout.decode()}")
            if stderr:
                print(f"Script error: {stderr.decode()}")
        except FileNotFoundError:
            print(f"The script was not found: {command[0]}")
        except Exception as e:
            print(f"Error occurred while running script: {e}")


    async def copy_exes_to_workers(self):
        """Runs the copy_exes_to_workers.sh script asynchronously using create_subprocess_exec."""
        script_path = '/home/chaskar2/distributed-logger/rainstorm/scripts/copy_exes_to_workers.sh'
        source_folder = '/home/chaskar2/distributed-logger/rainstorm/temp_files'
        target_folder = '/home/chaskar2/distributed-logger/rainstorm/exe_files'

        print(f"Running script: {script_path}")

        try:
            # Run the script using create_subprocess_exec for non-blocking execution
            process = await asyncio.create_subprocess_exec(
                script_path, 
                source_folder, 
                target_folder, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()  # Capture output and error

            if stdout:
                print(f"Script output: {stdout.decode()}")
            if stderr:
                print(f"Script error: {stderr.decode()}")
        except FileNotFoundError:
            print(f"The script {script_path} was not found.")
        except Exception as e:
            print(f"Error occurred while running script: {e}")


            

async def main():
    leader = Leader()
    
    # Run the server
    await leader.serve()


# For environments with an already running event loop
if __name__ == "__main__":
    try:
        # Try to use asyncio.run() if no event loop is running
        asyncio.run(main())
    except RuntimeError:  # If there's already a running event loop
        # Use the current event loop to run the main coroutine
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If the event loop is already running, create a task and run it
            loop.create_task(main())
        else:
            # If the event loop isn't running, run the coroutine normally
            loop.run_until_complete(main())