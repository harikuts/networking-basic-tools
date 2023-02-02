"""
Multi-connection Server (Event Driven-like Server)

Template provided by: Harikrishna Kuttivelil
(UC Santa Cruz, Internetworking Research Group)
"""

"""
This is an example of a multi-connection server you can implement using 
the knowledge and guide offered here:
https://realpython.com/python-sockets/#multi-connection-client-and-server.

This multi-connection server can accept connections from multiple clients 
connecting to it via a socket. This is helpful, for example, when you are 
expecting your server to handle multiple concurrent connections without 
blocking new connections. This template will mostly follow the code 
offered in the site above, but will have a few more annotations in it.

The "event-driven" aspect of it refers to the fact, that in this simple 
model, the server takes action on when it receives a message, an "event",
from a client. This is in contrast to the "request-response" model, where 
the server waits for a client to send a request, and then responds to it. 
To implement the latter, you may consider how we can flip the use of 
"servers" and "clients" to facilitate such a model.
"""

# Set up logging for server.
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)
slogger = logging.getLogger(f"(srv)")
slogger.setLevel(level=logging.INFO)

"""
Server Class:

Server class for listening to and replying to incoming messages. Pay 
special attention to where you can insert event-handling code.
"""
class Server:
    def __init__(self, host, port):
        """
        Our server will use a host (its own address) and port to listen 
        for connections. It must also use a selector to monitor for events 
        on the socket. It's good practice, when creating a class, to 
        initialize these variables in the __init__ function.
        """
        slogger.debug("Initializing server...")
        # Set up selector.
        self.sel = selectors.DefaultSelector()
        # Set host and port.
        self.host = host
        self.port = port
        slogger.info("Server initialized.")
    # Run function.
    def run(self):
        """
        Starts listening for connections and starts the main event loop. 
        This method works as the main "run" function for the server, 
        kicking off the other methods of this service.
        """
        slogger.debug("Starting server...")
        # Set, bind, and set to listen ports.
        slogger.debug("\tSetting socket...")
        """
        We first create a socket object, which is a connection endpoint 
        that can be used to send and receive data. In this case, we are 
        receiving and replying to messages from the "client". We then 
        set the socket to be reusable.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        """
        The socket is all set up and ready to listen for connections.
        """
        sock.listen()
        slogger.info(f"Listening from port {self.port}.")
        sock.setblocking(False)
        """
        As mentioned before, we use selectors to monitor for new events 
        by monitoring the socket for changes.
        """
        # Register the socket to be monitored.
        self.sel.register(sock, selectors.EVENT_READ, data=None)
        slogger.debug("Monitoring set.")
        """
        Finally, we arrive at the event loop. This is where the server
        will handle new incoming connections and their ensuing sessions.
        """
        # Event loop.
        try:
            while True:
                events = self.sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        """
                        Here, we accept and register new connections.
                        """
                        self.accept_wrapper(key.fileobj)
                    else:
                        """
                        Here, we service existing connections. This is 
                        the method where we will include our event-handling
                        code.
                        """
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
        """
        In this model, we are first waiting for new messages from the 
        client connections, so we are reading for data as it arrives 
        and until it stops arriving (end of message). We then switch 
        to writing to the client, sending a reply.
        """
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
                """
                EVENT HANDLING CODE HERE:

                At this point, you have received a complete message from 
                one of the client connections and it is stored in 
                data.outb. You can now handle the message however you 
                like. It is good practice to make a new method to handle 
                the message, and then call it here.
                """
                pass # ADD HANDLING CODE HERE.
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

"""
Using this simple server class, you should be able to create a server 
that can accept multiple socket connections from multiple clients and 
handle incoming messages.
"""