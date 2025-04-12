#!/usr/bin/env python3
import socket
import time
import argparse

def send_data_to_flink(host, port, delay=0.5):
    """
    Send test data to a Flink application via TCP socket
    
    Args:
        host: Hostname or IP address of the Flink server
        port: TCP port the Flink server is listening on
        delay: Delay in seconds between sending each row
    """
    print(f"Connecting to Flink application at {host}:{port}")
    
    try:
        # Create a socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"Connected successfully to {host}:{port}")
        
        # Sample data for each table
        test_data = [
            # Store sales data
            "store_sales|2451813|12345|1234|1000|1501|5001|201|401|10001|2|100.00|110.00|95.00|5.00|90.00|85.00|110.00|7.20|0.00|95.00|102.20|10.00",
            "store_sales|2451813|12346|5678|1001|1502|5002|202|402|10002|1|200.00|220.00|190.00|10.00|180.00|170.00|220.00|14.40|0.00|190.00|204.40|20.00",
            
            # Date dimension data
            "date_dim|2451813|AAAAAAAAOKJNECAA|2000-12-01|2400|350|801|2000|4|12|1|4|2000|801|350|Friday|2000Q4|N|N|N|2451783|2451813|2451447|2451475|N|N|N|N|N",
            "date_dim|2451814|AAAAAAAAPKJNECAA|2000-12-02|2400|350|801|2000|5|12|2|4|2000|801|350|Saturday|2000Q4|N|Y|N|2451783|2451813|2451448|2451475|N|N|N|N|N",
            
            # Item data
            "item|1234|AAAAAAAABAAAAAAA|1997-10-27||Item description for 1234|15.99|10.99|436|Brand #436|1|Category 1|12|Home|203|Manufacturer #203",
            "item|5678|AAAAAAAACAAAAAAA|1997-10-28||Item description for 5678|25.99|18.99|436|Brand #436|2|Category 2|15|Clothing|203|Manufacturer #203"
        ]
        
        # Send each line with a delay
        for line in test_data:
            print(f"Sending: {line[:60]}..." if len(line) > 60 else f"Sending: {line}")
            sock.sendall((line + "\n").encode('utf-8'))
            time.sleep(delay)  # Add a delay between sends
        
        print(f"Successfully sent {len(test_data)} rows of test data")
        
        # Keep the connection open briefly
        time.sleep(1)
        
    except ConnectionRefusedError:
        print(f"ERROR: Connection refused. Is the Flink application running and listening on {host}:{port}?")
    except Exception as e:
        print(f"ERROR: Failed to send data: {str(e)}")
    finally:
        try:
            sock.close()
            print("Connection closed")
        except:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send test data to Flink TCP socket")
    parser.add_argument("--host", default="10.0.2.83", help="Hostname or IP address of the Flink TaskManager (default: 10.0.2.64)")
    parser.add_argument("--port", type=int, default=8000, help="Port the Flink TaskManager is listening on (default: 8000)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between sending each row")
    
    args = parser.parse_args()
    send_data_to_flink(args.host, args.port, args.delay)