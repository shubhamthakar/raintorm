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
import time
import copy
import shlex
import base64


import rainstorm_pb2
import rainstorm_pb2_grpc

import worker_pb2
import worker_pb2_grpc


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
        self.execute_shell_script('./scripts/hydfs_run_all_worker.sh')

        # handle shutdown
        self.shutdown_flag = threading.Event()
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        self.stop_event = asyncio.Event()

        # Membership List
        self.membership_monitor_thread = threading.Thread(target=self.monitor_membership_list)
        self.membership_monitor_thread.start()

        # Logging
        self.log_file = '/home/chaskar2/distributed-logger/rainstorm/logs/leader.logs'
        self.init_logging()

        # Given list of worker nodes with their load (second value in the tuple)
        self.worker_load = [
            (0,'fa24-cs425-6902.cs.illinois.edu'),
            (0,'fa24-cs425-6903.cs.illinois.edu'),
            (0,'fa24-cs425-6904.cs.illinois.edu'),
            (0,'fa24-cs425-6905.cs.illinois.edu'),
            (0,'fa24-cs425-6906.cs.illinois.edu'),
            (0,'fa24-cs425-6907.cs.illinois.edu'),
            (0,'fa24-cs425-6908.cs.illinois.edu'),
            (0,'fa24-cs425-6909.cs.illinois.edu'),
            (0,'fa24-cs425-6910.cs.illinois.edu')
        ]
        # Heapify the list
        heapq.heapify(self.worker_load)

        # num of tasks per stage
        self.num_tasks = 0

        # store job related attributes
        self.task_mapping = defaultdict(dict)
        self.src_file = ""
        self.dest_file = ""

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
        
        
        # Appending source, special task

        print(f"Printing op_exes {op_exes}")
        # op_exes.append("source")

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

                self.log(f"min server extracted: {min_server} with load {load}")

                # Check if the server is in the membership list
                server_found = False
                for node in self.hydfs_node.process.membership_list:
                    if node['node_id'].startswith(server_ip) and node['status'] == 'LIVE':  # Check if the IP matches
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
            self.task_mapping[op] = assigned_servers

        return self.task_mapping


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

        self.log("Logging initialized for Rainstorm Leader")

    
    def extract_servers_and_ports_from_mapping(self):
        
        server_list = []
        port_list = []

        for task, servers in self.task_mapping.items():
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

        self.src_file = request.hydfs_src_file
        self.dest_file = request.hydfs_dest_filename

        # Save and set permissions for the op_exes
        for i, binary in enumerate(request.op_exes):
            op_path = os.path.join(self.base_dir, f"{request.op_exe_names[i]}")
            print("Here")
            with open(op_path, "wb") as f:
                f.write(binary)
            os.chmod(op_path, 0o777)
        

        ops_with_source = ["source"]
        ops_with_source.extend(request.op_exe_names)

        self.task_mapping = await self.create_task_mapping(ops_with_source, request.num_tasks)

        # Copy op exes to all the workers asynchronously
        await self.copy_exes_to_workers()

        # Get servers and ports to start the workers
        servers, ports = self.extract_servers_and_ports_from_mapping()

        # Start worker processes asynchronously
        await self.start_rainstorm_workers( src_file, dest_file, ports, servers)

        return rainstorm_pb2.JobResponse(message="Job submitted successfully!")


    def shutdown(self, signum, frame):
        """ Graceful shutdown function """
        self.log("Shutting down Rainstorm Leader ...")
        self.shutdown_flag.set()  # Signal the listener thread to stop
        self.hydfs_node.shutdown(signum, frame)

        if self.membership_monitor_thread.is_alive():
            self.log("Membership thread is still running; proceeding to join...")
            self.membership_monitor_thread.join()  # Wait for the listener thread to finish
            self.log("Membership thread joined successfully.")

        self.log("Rainstorm Leader shut down gracefully.")
        self.stop_event.set()

        # shutdown rainstorm workers
        self.execute_shell_script('./scripts/rainstorm_worker_cleanup.sh')

        # shutdown HyDFS
        self.execute_shell_script('./scripts/hydfs_process_cleanup.sh')

        
    async def monitor_shutdown_flag(self):
        """Monitor threading.Event and set asyncio.Event when triggered."""
        print("Monitoring threading.Event for shutdown...")
        await asyncio.to_thread(self.shutdown_flag.wait)  # Wait for threading.Event
        print("threading.Event triggered. Setting asyncio.Event...")
        self.stop_event.set()
        

    def execute_shell_script(self, shell_script):
        """Runs the hydfs_run_all_worker.sh script."""

        try:
            print(f"Running script: {shell_script}")
            result = subprocess.run([shell_script], check=True, text=True, capture_output=True)
            print("Script output:", result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Script failed with error: {e}")
            print("Error output:", e.stderr)
        except FileNotFoundError:
            print(f"The script {shell_script} was not found.")




    async def start_rainstorm_workers(self, src_file, dest_file, ports, servers):
        """Starts the rainstorm workers by running the start_rainstorm_workers.sh script asynchronously using create_subprocess_exec."""
        # Convert mapping dictionary to JSON string
        mapping_dict_json = json.dumps(self.task_mapping, ensure_ascii=True)
        mapping_dict_json = base64.b64encode(mapping_dict_json.encode()).decode()  # Escape quotes for shell

        print(f"Encoded mapping: {mapping_dict_json}")

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

    

    def sync_start_rainstorm_workers(self, src_file, dest_file, ports, servers):
        """Starts the rainstorm workers by running the start_rainstorm_workers.sh script synchronously."""
        # Convert mapping dictionary to JSON string
        # Convert mapping dictionary to JSON string
        mapping_dict_json = json.dumps(self.task_mapping, ensure_ascii=True)
        mapping_dict_json = base64.b64encode(mapping_dict_json.encode()).decode()  # Escape quotes for shell

        print(f"Encoded mapping from Synchronous Worker Starting Function: {mapping_dict_json}")

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
            # Run the script synchronously
            print(f"Running script: {command}")
            result = subprocess.run(
                command, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True  # Ensures output is returned as string
            )

            # Handle stdout and stderr
            if result.stdout:
                print(f"Script output: {result.stdout}")
            if result.stderr:
                print(f"Script error: {result.stderr}")

            # Raise an exception if the script failed
            if result.returncode != 0:
                raise RuntimeError(f"Script failed with return code {result.returncode}")
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


    def handle_node_change(self, current_node, action):
        """
        Handle changes to the cluster when a node dies or joins.

        Parameters:
        current_node (str): The node that changed.
        action (str): "dead" if the node has shut down, "joined" if a node joins.
        """

        self.log(f"handle node change called for action: {action}")

        if action == "dead":
            self.log(f"Node {current_node} is dead. Reassigning tasks...")

            # Filter alive workers from self.worker_load to remove the matching element
            self.worker_load = [
                node for node in self.worker_load if not current_node['node_id'].startswith(node[1])
            ]

            # Maintain a dictionary to track current tasks per node
            node_task_count = {node: 0 for _, node in self.worker_load}
            for tasks in self.task_mapping.values():
                for assigned_node in tasks.values():
                    node, port = assigned_node.split(":")
                    if node in node_task_count:
                        node_task_count[node] += 1

            # Keep a copy of the old task mapping
            old_task_mapping = self.task_mapping.copy()

            # Remove tasks associated with the dead node from self.task_mapping
            updated_task_mapping = {}
            tasks_to_reassign = []  # List of tasks that need reassignment

            for stage, tasks in self.task_mapping.items():
                updated_task_mapping[stage] = {}
                for task_id, assigned_node in tasks.items():
                    node, port = assigned_node.split(":")
                    if current_node['node_id'].startswith(node):
                        # Collect tasks to reassign
                        tasks_to_reassign.append((stage, task_id))
                    else:
                        # Keep existing assignments
                        updated_task_mapping[stage][task_id] = assigned_node

            # Reassign tasks to the most underutilized nodes
            new_node_ports = []  # To track new node:port pairs
            for stage, task_id in tasks_to_reassign:
                if not self.worker_load:
                    raise RuntimeError("No available nodes to reassign tasks!")

                # Get the most underutilized node
                least_loaded_node = heapq.heappop(self.worker_load)
                new_node, new_load = least_loaded_node[1], least_loaded_node[0]

                # Assign the task to the new node
                next_port = 5002 + node_task_count[new_node]
                new_node_port = f"{new_node}:{next_port}"
                updated_task_mapping[stage][task_id] = new_node_port

                # If the node:port pair is new, track it
                if new_node_port not in old_task_mapping.get(stage, {}).values():
                    new_node_ports.append(new_node_port)

                # Update the task count and node's load
                node_task_count[new_node] += 1
                heapq.heappush(self.worker_load, (new_load + 1, new_node))

            # Update the self.task_mapping with reassigned tasks
            old_task_mapping = self.task_mapping
            self.task_mapping = updated_task_mapping
            self.log(f"New Task Mapping: {self.task_mapping}")
            self.log("Task mapping updated successfully.")

            # Start workers only on new node:port pairs
            for new_node_port in new_node_ports:
                node, port = new_node_port.split(":")
                self.log(f"Starting worker on new node:port pair: {new_node_port}")
                src_file = self.src_file  # Replace with actual source file path
                dest_file = self.dest_file # Replace with actual destination file path
                ports = [port]
                servers = [node]
                self.sync_start_rainstorm_workers(src_file, dest_file, ports, servers)

            # Notify all workers about the updated mapping
            self.notify_workers(self.task_mapping, old_task_mapping)

            return self.task_mapping

        elif action == "joined":
            self.log(f"Node {current_node} joined. Assigning tasks to the new node...")

            # For now, handle newly joined nodes as idle nodes unless specific tasks are assigned
            self.log(f"Node {current_node} joined. No immediate tasks reassigned.")


    
    def notify_workers(self, updated_task_mapping, old_task_mapping): 
        """
        Notify relevant workers about the updated task mapping via gRPC.

        Parameters:
        updated_task_mapping (dict): The updated task mapping to send to workers.
        """
        # Extract all worker addresses and ports from the updated task mapping
        worker_addresses = set()  # Use a set to avoid duplicates
        existing_worker_addresses = set() # Use this to get existing workers

        for task_type, task_map in old_task_mapping.items():
            for node_index, node_address in task_map.items():
                # The port for each worker is included in the task mapping values
                existing_worker_addresses.add(node_address)

        for task_type, task_map in updated_task_mapping.items():
            for node_index, node_address in task_map.items():
                # The port for each worker is included in the task mapping values
                worker_addresses.add(node_address)

        workers_to_notify = set()
        workers_to_notify = worker_addresses & existing_worker_addresses

        self.log(f"Workers to notify: {workers_to_notify}")

        # Now notify only the workers that are part of the updated task mapping
        for node_address in workers_to_notify:
            try:
                # Assuming the worker address in the mapping contains the full address (hostname:port)
                host, port = node_address.split(':')

                # Create a synchronous gRPC channel and stub with the correct port
                with grpc.insecure_channel(f"{host}:{port}") as channel:
                    stub = worker_pb2_grpc.WorkerStub(channel)

                    # Create the request message
                    request = worker_pb2.DataRequest(data=json.dumps(updated_task_mapping))

                    # Call the update_mapping RPC synchronously
                    response = stub.RecvData(request)
                    self.log(f"Worker {node_address} acknowledged: {response.ack}")
            except Exception as e:
                self.log(f"Failed to notify worker {node_address}: {e}")





    def monitor_membership_list(self):
        """
        Monitor the membership list for changes. If a new node joins or a node status changes to 'DEAD',
        find its 2 predecessors and 1 successor.
        """
        time.sleep(20)
        self.log("Rainstorm Tasks: Monitoring membership list started")
        previous_membership_list = copy.deepcopy(self.hydfs_node.process.membership_list)

        while not self.shutdown_flag.is_set():
            # Check for changes in the membership list every 2 seconds
            time.sleep(2)
            current_membership_list = copy.deepcopy(self.hydfs_node.process.membership_list)

            if previous_membership_list != current_membership_list:
                self.log(f"Rainstorm Tasks: Membership lists are not equal")
                for current_node in current_membership_list:
                    if not any(previous_node['node_id'] == current_node['node_id']
                            for previous_node in previous_membership_list):
                        # New node detected
                        self.log(f"Rainstorm Tasks: New node detected: {current_node}")
                        self.task_mapping = self.handle_node_change(current_node, "joined")

                    elif current_node['status'] == 'DEAD' and any(
                            previous_node['node_id'] == current_node['node_id'] and previous_node['status'] != 'DEAD'
                            for previous_node in previous_membership_list):
                        # Node status changed to DEAD
                        self.log(f"Rainstorm Tasks: Hydfs node marked as DEAD: {current_node}")
                        try:
                            # time.sleep(2*self.hydfs_node.process.protocol_period)
                            self.task_mapping = self.handle_node_change(current_node, "dead")
                        except Exception as e:
                            self.log(f"Rainstorm Tasks: Exception caught in thread: {e}")

                previous_membership_list = copy.deepcopy(current_membership_list)
            
            self.log("Rainstorm Tasks: No change in membership list")


            

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