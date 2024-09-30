import psutil
import time

def get_network_bandwidth(interface='eth0'):
    """Function to get bytes sent and received over a network interface."""
    net_io = psutil.net_io_counters(pernic=True)[interface]
    return net_io.bytes_sent, net_io.bytes_recv

def measure_bandwidth(interval=1, interface='eth0'):
    """Measure bandwidth usage over a given time interval."""
    sent1, recv1 = get_network_bandwidth(interface)
    time.sleep(interval)
    sent2, recv2 = get_network_bandwidth(interface)

    bytes_sent = sent2 - sent1
    bytes_recv = recv2 - recv1

    bandwidth_in_bytes_per_sec = (bytes_sent + bytes_recv) / interval
    return bandwidth_in_bytes_per_sec, bytes_sent, bytes_recv

if __name__ == "__main__":
    # Define the network interface (default is 'eth0')
    interface = input("Enter the network interface to monitor (default 'eth0'): ") or 'eth0'
    
    # Define the interval for measuring bandwidth (in seconds)
    interval = float(input("Enter the measurement interval in seconds (default 1 sec): ") or 1)

    print(f"Monitoring bandwidth on interface '{interface}' every {interval} seconds...")
    print(f"{'Time':<20}{'Total Bandwidth (bytes/sec)':<30}{'Bytes Sent':<15}{'Bytes Received':<15}")

    try:
        while True:
            bandwidth, sent, recv = measure_bandwidth(interval, interface)
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()):<20}{bandwidth:<30}{sent:<15}{recv:<15}")
    except KeyboardInterrupt:
        print("\nBandwidth monitoring stopped.")

