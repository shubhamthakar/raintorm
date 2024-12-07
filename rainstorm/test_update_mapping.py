import grpc
import json
import worker_pb2
import worker_pb2_grpc

def test_update_mapping():
    # Address of the worker to test
    worker_address = "fa24-cs425-6901.cs.illinois.edu:5002"
    
    # Test mapping dictionary
    test_mapping = {
        "source": {
            "0": "fa24-cs425-6902.cs.illinois.edu:5002"
        }
    }
    
    # Convert the mapping to a JSON string
    mapping_json = json.dumps(test_mapping)

    try:
        # Create a gRPC channel
        with grpc.insecure_channel(worker_address) as channel:
            # Create a stub
            stub = worker_pb2_grpc.WorkerStub(channel)
            
            # Create the request object
            request = worker_pb2.MappingUpdateRequest(mapping=mapping_json)
            
            # Call the UpdateMapping endpoint
            response = stub.UpdateMapping(request)
            
            # Print the response
            print(f"Response from UpdateMapping: {response.status}")
    except grpc.RpcError as e:
        print(f"gRPC call failed: {e.details()}")
        print(f"Debug information: {e.debug_error_string()}")

if __name__ == "__main__":
    test_update_mapping()

