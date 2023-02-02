import selectors
import select
import types
import socket
import struct
import queue
import logging

# Set up logging for server.
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)
slogger = logging.getLogger(f"(srv)")
slogger.setLevel(level=logging.INFO)
# Set up logging for connector.
clogger = logging.getLogger(f"(conn)")
clogger.setLevel(level=logging.INFO)

USE_NSB = True
NSB_SERVER_ADDR = "host.docker.internal"
# Set the port that this service will use.
SERVER_PORT = 65432

# Get own IP address.
this_ip = socket.gethostbyname(socket.gethostname())

# Define message types.
SEND = 0
ACK = 1
REQ = 2
MSG = 3

# Create a struct that formats IPC messages with SEND/ACK/REQ/MSG, source IP address, and destination IP address.
IPCMessage = struct.Struct('! I 4s 4s')
# Get the size of the IPC message.
IPCMessage_size = IPCMessage.size
# Define function that packs a message with variable length if necessary.
def pack_message(command, src_addr, dest_addr, payload: bytes):
    """
    Packs a message with a command, address, and payload.
    """
    # Pack the message.
    if src_addr is not None:
        src_addr_bytes = socket.inet_aton(src_addr)
    else:
        src_addr_bytes = b'\x00\x00\x00\x00'
    if dest_addr is not None:
        dest_addr_bytes = socket.inet_aton(dest_addr)
    else:
        dest_addr_bytes = b'\x00\x00\x00\x00'
    packed_message = IPCMessage.pack(command, src_addr_bytes, dest_addr_bytes) + payload
    # Return the packed message.
    return packed_message
# Define function that unpacks a message with variable length if necessary.
def unpack_message(message: bytes):
    """
    Unpacks a message with a command, address, and payload.
    """
    # Unpack the message.
    command, src_addr_bytes, dest_addr_bytes = IPCMessage.unpack(message[:IPCMessage_size])
    # Convert address bytes to strings.
    if src_addr_bytes == b'\x00\x00\x00\x00':
        src_addr = None
    else:
        src_addr = socket.inet_ntoa(src_addr_bytes)
    if dest_addr_bytes == b'\x00\x00\x00\x00':
        dest_addr = None
    else:
        dest_addr = socket.inet_ntoa(dest_addr_bytes)
    # Get the message.
    payload = message[IPCMessage_size:]
    # Return components.
    return command, src_addr, dest_addr, payload

# Function to shorten payload when printing.
def short(payload: bytes):
    """
    Shortens a payload for printing, includes length of payload.
    """
    if len(payload) > 10:
        return payload[:10] + b"..." + b" (" + str(len(payload)).encode() + b" bytes)"
    else:
        return payload + b" (" + str(len(payload)).encode() + b" bytes)"

"""
Server Class:

Server class for listening to and replying to incoming messages.
"""
class Server:
    def __init__(self, host, port):
        slogger.debug("Initializing server...")
        # Set up selector.
        self.sel = selectors.DefaultSelector()
        # Set host and port.
        self.host = host
        self.port = port
        # Set up a queue.
        self.queue = queue.Queue()
        slogger.info("Server initialized.")
    # Run function.
    def run(self):
        """
        Starts listening for connections and starts the main event loop.
        """
        slogger.debug("Starting server...")
        # Set, bind, and set to listen ports.
        slogger.debug("\tSetting socket...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen()
        slogger.info(f"Listening from port {self.port}.")
        sock.setblocking(False)
        # Register the socket to be monitored.
        self.sel.register(sock, selectors.EVENT_READ, data=None)
        slogger.debug("Monitoring set.")
        # Event loop.
        try:
            while True:
                events = self.sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        self.accept_wrapper(key.fileobj)
                    else:
                        self.service_connection(key, mask)
        except KeyboardInterrupt:
            slogger.info("Caught keyboard interrupt, exiting...")
        finally:
            self.sel.close()
    # Helper functions for accepting wrappers, servicing connections, and closing.
    def accept_wrapper(self, sock):
        """
        Accepts and registers new connections.
        """
        conn, addr = sock.accept()
        slogger.debug(f"Accepted connection from {addr}.")
        # Disable blocking.
        conn.setblocking(False)
        # Create data object to monitor for read and write availability.
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        # Register connection with selector.
        self.sel.register(conn, events, data=data)
    def service_connection(self, key:selectors.SelectorKey, mask):
        """
        Services the existing connection and calls to unregister upon completion.
        """
        slogger.debug(f"Servicing connection from: {key}, {mask}")
        sock = key.fileobj
        data = key.data
        # Check for reads or writes.
        if mask & selectors.EVENT_READ:
            # At event, it should be ready for read.
            recv_data = sock.recv(1024)
            # As long as data comes in, append it.
            if recv_data:
                data.outb += recv_data
            # When data stops, close the connection.
            else:
                slogger.debug(f"Closing connection to {data.addr}")
                self.sel.unregister(sock)
                sock.close()
        if mask & selectors.EVENT_WRITE:
            # At event, it should be ready to write.
            if data.outb:
                # Add source and bytes to queue.
                self.handleInteraction(data.outb, sock)
                # Unregister and close socket.
                self.unregister_and_close(sock)
    def unregister_and_close(self, sock:socket.socket):
        """
        Unregisters and closes the connection, called at the end of service.
        """
        slogger.debug("Closing connection...")
        # Unregister the connection.
        try:
            self.sel.unregister(sock)
        except Exception as e:
            slogger.error(f"Socket could not be unregistered:\n{e}")
        # Close the connection.
        try:
            sock.close()
        except OSError as e:
            slogger.error(f"Socket could not close:\n{e}")
    def handleInteraction(self, message : bytes, sock):
        """
        Handles an interaction from a client.
        """
        slogger.debug(f"Handling interaction from {sock.getpeername()[0]}: {short(message)}")
        # Unpack the message.
        command, src_addr, dest_addr, payload = unpack_message(message)
        # Check the command.
        if command == SEND:
            slogger.debug(f"Processing SEND command from {src_addr}...")
            # SEND here means that it is being received, so add it to the queue.
            slogger.debug(f"Storing sent message from {src_addr}: {payload}")
            self.queue.put((src_addr, payload))
            slogger.info(f"Message stored from {src_addr}.")
            # Send an ACK back.
            reply_message = pack_message(ACK, src_addr, dest_addr, b'')
            sock.sendall(reply_message)
            slogger.debug(f"ACK sent to {src_addr}.")
        elif command == REQ:
            slogger.debug(f"Processing REQ command from {src_addr}...")
            # REQ is a request for a message, so send the next message in the queue.
            if not self.queue.empty():
                slogger.debug(f"Message(s) available, forwarding message to {src_addr}...")
                # Get the next message.
                src_addr, payload = self.queue.get()
                # Pack the message.
                message = pack_message(MSG, src_addr, this_ip, payload)
                # Send the message.
                sock.sendall(message)
                slogger.info(f"Message forwarded to {src_addr}.")
            else:
                slogger.debug(f"No messages available, sending empty message to {src_addr}...")
                # If the queue is empty, send an empty message.
                message = pack_message(MSG, None, None, b"")
                sock.sendall(message)
                slogger.debug(f"Empty message sent to {src_addr}.")

"""
Connector Functions:

User-facing functions for interfacing with the real server or NSB.
"""
# class Connector:
#     def __init__(self, server_host, server_port):
#         clogger.debug("Initializing connector...")
#         # Set host and port of the server.
#         self.server_host = server_host
#         self.server_port = server_port
#         # Create a socket.
#         # self.sock = socket.socket()
#         # # Connect to the server.
#         # self.sock.connect((self.server_host, self.server_port))
#         # # Set the socket to non-blocking.
#         # self.sock.setblocking(False)
#         # clogger.debug(f"Connected to {self.sock.getpeername()[0]}")
#         clogger.info("Connector initialized.")

"""
NSB Interface:

Create a wrapper connector for the NSB interface.
"""
import nsb_api.nsb_payload as nsbp
import nsb_api.nsb_client as nsb
class NSBWrapper:
    def __init__(self, server_addr):
        self.nsb = nsb.NSBApplicationClient(server_addr=server_addr)
    def send(self, dest_addr, payload: bytes):
        """
        Sends a message to the server, and waits for acknowledgment.
        """
        # Get source address.
        src_addr = self.nsb.local_ip
        # Send the message.
        self.nsb.send(src_addr, dest_addr, payload)
        clogger.info(f"Message sent to {dest_addr}: {short(payload)}")
    def receive(self):
        """
        Polls the server for a message from a given source address.
        """
        # Get destination address.
        dest_addr = self.nsb.local_ip
        # Poll the server for a message.
        reply = self.nsb.receive(dest_addr)
        if reply is None:
            clogger.debug(f"No message received at {dest_addr}.")
            return None,None
        else:
            src_addr, dest_addr, payload = reply
            clogger.info(f"Message received from {src_addr} to {dest_addr}: {short(payload)}")
            return src_addr, payload

if USE_NSB and __name__ != "__main__":
    """
`   If using NSB, use the NSB wrapper.
    """
    # Get wrapper for NSB.
    connector = NSBWrapper(NSB_SERVER_ADDR)
    # Set send and receive functions.
    send = connector.send
    receive = connector.receive
else:
    """
    If not using NSB, use the real server, which will be active during runtime.
    """
    def send(dest_addr, payload: bytes):
        """
        Sends a message to the server, and waits for acknowledgment.
        """
        clogger.debug(f"Sending to {dest_addr}: {short(payload)}")
        try:
            # Connect to the server.
            sock = socket.socket()
            sock.connect((dest_addr, SERVER_PORT))
            clogger.debug(f"Connected {sock.getsockname()[0]} to {sock.getpeername()[0]}")
        except Exception as e:
            raise(ConnectionError("Connection error: " + str(e)))
        message = pack_message(SEND, this_ip, dest_addr, payload)
        # Send the message.
        clogger.debug(f"Sending message to {dest_addr}: {short(message)}")
        sock.sendall(message)
        clogger.info(f"Message sent to {dest_addr}: {short(payload)}")
        # Wait for acknowledgment, or timeout.
        ready = select.select([sock], [], [], 3)
        if ready[0]:
            # Receive the acknowledgment.
            data = sock.recv(1024)
            clogger.debug(f"Received data: {short(data)}")
            # Unpack the message.
            command, src_addr, dest_addr, payload = unpack_message(data)
            # Check the command.
            if command == ACK:
                clogger.debug(f"\tMessage acknowledged by {dest_addr}.")
                return
            else:
                raise AckTimeout("\tUnknown acknowledgment issue.")
        else:
            raise AckTimeout("\tNo acknowledgment received, timed out after 3 seconds.")
        sock.close()
    def receive():
        """
        Polls the server for a message from a given source address.
        """
        clogger.debug("Receiving...")
        try:
            # Connect to the server.
            sock = socket.socket()
            sock.connect((this_ip, SERVER_PORT))
            # # Set the socket to non-blocking.
            # sock.setblocking(False)
            clogger.debug(f"Connected to {sock.getpeername()[0]}")
        except Exception as e:
            raise(ConnectionError(f"Connection error: {e}"))
        # Pack the message.
        message = pack_message(REQ, this_ip, None, b'')
        # Send the message.
        sock.sendall(message)
        clogger.debug(f"Request sent to {sock.getsockname()[0]}:{SERVER_PORT}")
        # Wait for a response and add it to the data by chunks.
        ready = select.select([sock], [], [], 3)
        if ready[0]:
            # Receive data by chunks.
            data = b''
            while True:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                data += chunk
            clogger.debug(f"Received data: {short(data)}")
            # Unpack the message.
            command, src_addr, dest_addr, payload = unpack_message(data)
            clogger.debug(f"\tUnpacked: {command} from {src_addr}-->{dest_addr}: {short(payload)}")
            # Check the command.
            if command == MSG:
                # If payload is empty, return None.
                if payload == b'' and src_addr == None:
                    clogger.debug(f"No message received.")
                    sock.close()
                    return None, None
                else:
                    clogger.info(f"Message received from {src_addr} to {dest_addr}: {short(payload)}")
                    sock.close()
                    return src_addr, payload
            else:
                sock.close()
                raise ReceiveError("Unknown receive issue.")
        else:
            sock.close()
            raise ReceiveError("No message received, timed out after 3 seconds.")
        sock.close()

    # def close(self):
    #     """
    #     Closes the connection.
    #     """
    #     self.sock.close()
    #     clogger.debug("Connection closed.")

# Define errors.
class ConnectionError(Exception):
    pass
class AckTimeout(Exception):
    pass
class ReceiveError(Exception):
    pass
class CommandError(Exception):
    pass

# Run the server.
if __name__ == "__main__":
    if USE_NSB:
        import time
        print("Running NSB server...")
        # Just keep process alive.
        while True:
            time.sleep(1)
    else:
        print("Running server...")
        # Print local IP.
        print(socket.gethostbyname(socket.gethostname()))
        # Create the server from this IP address and specified port.
        server = Server(socket.gethostbyname(socket.gethostname()), 65432)
        # Run the server.
        server.run()