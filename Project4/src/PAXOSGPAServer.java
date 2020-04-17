
/**
 * Multithreaded Java RMI HashMap server with 2 phase commit among multiple server instances
 * Supports int Student ID keys and float GPA values
 * Operations are Put(key, value), Get(key), and Delete(key)
 * CS 6650 Scalable Distributed Systems
 * Spring 2020 Project 3
 * 3/25/20
 * by Rohan Subramaniam
 */

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;

public class PAXOSGPAServer extends UnicastRemoteObject implements src.GPARepo, Runnable {
    // Hashmap/dictionary of the ID and GPAs. This is the synchronized resource
    private final HashMap<Integer, Float> map = new HashMap<>();

    // Coordinator connection variables
    private Socket serverSocket = null;
    private DataOutputStream dataOut = null;
    private DataInputStream dataIn = null;
    private static boolean closed = false;
    private int coordPort = 1235;
    private String coordHost = "localhost";

    // Codes for the server roles in PAXOS
    public final int ROLE_PROPOSER = 1;
    public final int ROLE_ACCEPTOR = 2;
    public final int ROLE_LEARNER = 3;

    public int currentRole;
    public long maxID;
    public long proposeID;
    public long acceptedID;
    public String acceptedVal = null;

    /**
     * Constructor that binds the server to the specified host and port
     * @param port int port. 8080 by default
     * @param host String hostname. localhost by default
     * @throws RemoteException if the host is invalid or unavailable
     */
    public PAXOSGPAServer(int port, String host) throws RemoteException {
        try {
            currentRole = ROLE_ACCEPTOR;
            maxID = 0;
            System.out.println(timestamp() + "Starting server on Host: " + host + "  Port #" + port);
            LocateRegistry.createRegistry(port);
            Naming.rebind("rmi://" + host + ":" + port + "/GPAService", this);
            System.out.println(timestamp() + "GPA Server bound in registry");
        } catch (Exception e) {
            System.out.println(timestamp() + "GPA Server error: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Constructor that takes in a role argument if specified by the user
     * @param port int server port
     * @param host String hostname
     * @param role int 1, 2, or 3 depending on role specified. Made to specify learners
     * @throws RemoteException If the RMI binding doesn't work
     */
    public PAXOSGPAServer(int port, String host, int role) throws RemoteException {
        try {
            currentRole = role;
            maxID = 0;
            System.out.println(timestamp() + "Starting server on Host: " + host + "  Port #" + port);
            LocateRegistry.createRegistry(port);
            Naming.rebind("rmi://" + host + ":" + port + "/GPAService", this);
            System.out.println(timestamp() + "GPA Server bound in registry");
        } catch (Exception e) {
            System.out.println(timestamp() + "GPA Server error: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Main server method that reads the host and port args and starts the server.
     * @param args hostname and port. Role if desired
     */
    public static void main(String[] args) {
        if (args.length != 2 && args.length != 3) {
            System.out.println("Invalid number of arguments");
            System.out.println("To specify port and host use java -jar src.RMIGPAServer.jar <hostname> <port>");
            System.exit(1);
        } else {
            try {
                int port = Integer.parseInt(args[1]);
                String host = args[0];
                Thread server;
                if (args.length == 3) {
                    int role = Integer.parseInt(args[2]);
                    if (role <= 3) {
                        server = new Thread(new PAXOSGPAServer(port, host, role));
                    } else {
                        System.out.println("Invalid Role argument. Defaulting to ACCEPTOR");
                        server = new Thread(new PAXOSGPAServer(port, host));
                    }
                } else {
                    server = new Thread(new PAXOSGPAServer(port, host));
                }
                server.start();

            } catch (NumberFormatException e) {
                System.out.println("Invalid port argument. Port number must be an int");
                System.exit(1);
            } catch (RemoteException e) {
                System.out.println("RemoteException starting server. Invalid host or port");
                System.exit(1);
            }
        }
    }

    /**
     * Thread run method. Establishes connection to the Coordinator and listens for PAXOS instructions
     */
    @Override
    public void run() {
        // Create the socket connection to the coordinator and establish streams
        try {
            serverSocket = new Socket(coordHost, coordPort);
            dataOut = new DataOutputStream(serverSocket.getOutputStream());
            dataIn = new DataInputStream(serverSocket.getInputStream());
        } catch (IOException e) {
            System.out.println(timestamp() + "IOException occurred while establishing coordinator socket and streams");
            System.exit(1);
        }

        try {
            if (currentRole == ROLE_LEARNER) {
                dataOut.writeUTF("LEARNER");
            }
        } catch (IOException e) {
            System.out.println("IOException while informing Coordinator of role");
        }

        // Loop to process PAXOS communication
        String coordMessage;
        while (!closed) {
            if (currentRole == ROLE_PROPOSER) {
                sleepThread(2000);
                continue;
            }

            try {
                coordMessage = dataIn.readUTF();
                System.out.println("Message from coordinator at beginning of while loop: " + coordMessage);
                String[] params = coordMessage.split("[( ),]+", 4);

                if (coordMessage.startsWith("PREPARE") && currentRole == ROLE_ACCEPTOR) {
                    System.out.println(timestamp() + "Received PAXOS PREPARE. Proposal ID: " + params[1]);
                    proposeID = Long.parseLong(params[1]);

                    if (proposeID < maxID) {
                        dataOut.writeUTF("IGNORED");
                    } else {
                        maxID = proposeID;

                        if (acceptedVal != null) {
                            dataOut.writeUTF("PROMISE " + proposeID + " ACCEPTED " + acceptedID + " " + acceptedVal);
                            System.out.println(timestamp() + "Sending PROMISE with previous accepted val: " + acceptedVal);
                        } else {
                            dataOut.writeUTF("PROMISE " + proposeID);
                            System.out.println(timestamp() + "Sending PROMISE");
                        }
                    }

                } else if (coordMessage.startsWith("ACCEPT")) {
                    System.out.println(timestamp() + "Received ACCEPT " + params[2] + " from proposer");
                    // Message format: "ACCEPT REQUEST id [put,k,v]"
                    // params format: ["ACCEPT", "REQUEST", "id", "[PUT,K,V]"
                    System.out.println("Coord message: " + coordMessage);
                    System.out.println("Split coord message: " + Arrays.toString(params));
                    if (Long.parseLong(params[2]) < maxID) { // Check if the ID is lower than promised
                        System.out.println("IGNORED ACCEPT");
                        dataOut.writeUTF("IGNORED");
                    } else {
                        dataOut.writeUTF("ACCEPT " + params[2] + " " + params[3]);
                        System.out.println(timestamp() + "ACCEPTED value: " + params[3]);
                        acceptedVal = params[3];
                    }
                } else if (coordMessage.startsWith("DONE")) {
                    resetPAXOS();
                } else if (coordMessage.startsWith("[put")) {
                    System.out.println("Wrong loop is catching the final request");
                }
            } catch (IOException e) {
                System.out.println(timestamp() + "IOException handling PAXOS messages. Coordinator not available");
                System.exit(1);
            }
        }
    }

    /**
     * Helper function to execute the actual put or delete after a confirmed Global Commit from the coordinator.
     * @param params String array of the 2PC command
     * @throws IOException if the Coordinator is unavailable
     */
    private void execute(String[] params) throws IOException {
        if (params[1].equalsIgnoreCase("PUT")) {
            map.put(Integer.parseInt(params[2]), Float.parseFloat(params[3]));
        }

        if (params[1].equalsIgnoreCase("DELETE")) {
            map.remove(Integer.parseInt(params[2]));
        }
    }

    /**
     * Put method for the GPA server hashmap. Initiates PAXOS by using request()
     * @param key int Student ID
     * @param val float GPA
     * @throws RemoteException if the RPC fails
     */
    @Override
    public String put(int key, float val) throws RemoteException {
        String param = "[put," + key + "," + val + "]";
        return request(param);
    }

    /**
     * Get method for the hashmap. Retrieves the value of a specified key
     * @param key int Student ID
     * @return float GPA of the student
     * @throws RemoteException if the RPC fails
     */
    @Override
    public String get(int key) throws RemoteException {
        String param = "[get," + key + "]";
        return request(param);
    }

    /**
     * Deletes a given key from the hashmap
     * @param key int StudentID
     * @return boolean true if success false if the key wasn't found
     * @throws RemoteException if RPC fails
     */
    @Override
    public String delete(int key) throws RemoteException {
        String param = "[delete," + key + "]";
        return request(param);
    }

    /**
     * Used by put/get/delete to initiate PAXOS with a request that leads to a PREPARE
     * @param param String The full command
     * @return String response
     */
    private String request(String param) {
        synchronized (map) {
            System.out.println(timestamp() + "Received request from client: " + param);
            currentRole = ROLE_PROPOSER;

            proposeID = System.nanoTime();
            try {
                // Format will be "REQUEST ID [put,k,v]"
                dataOut.writeUTF("REQUEST " + proposeID + " " + param);
            } catch (IOException e) {
                System.out.println(timestamp() + "IOException while writing request to coordinator");
            }

            while (true) { // add timeout here?
                try {
                    String coordMessage = dataIn.readUTF();
                    System.out.println("In request() while loop coord message: " + coordMessage);
                    String[] command = coordMessage.split("[\\[\\]( ),]+");
                    if (command[0].equalsIgnoreCase("put")) {
                        map.put(Integer.parseInt(command[1]), Float.parseFloat(command[2]));
                        return "Successfully put key: " + Integer.parseInt(command[1]) + " Value: " +
                                Float.parseFloat(command[2]);
                    } else if (command[0].equalsIgnoreCase("get")) {
                        float val = map.get(Integer.parseInt(command[1]));
                        return "Key: " + Integer.parseInt(command[1]) + " Value is: " + val;
                    } else if (command[0].equalsIgnoreCase("delete")) {
                        map.remove(Integer.parseInt(command[1]));
                        return "Successfully removed key: " + Integer.parseInt(command[1]);
                    }
                } catch (IOException e) {
                    System.out.println("IOException while finalizing accepted command");
                    System.exit(1);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("IndexOutOfBoundsException occurred while finalizing command. Invalid syntax");
                }

            }
        }
    }

    /**
     * Resets values for another round of PAXOS
     */
    private void resetPAXOS() {
        maxID = 0;
        proposeID = 0;
        acceptedID = 0;
        acceptedVal = null;
    }

    /**
     * Timestamp of the current time to print on each line.
     * @return String version of the timestamp formatted for readability
     */
    private String timestamp() {
        long currentTime = System.currentTimeMillis();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:ms");
        //formatted value of current Date
        String time = "" + df.format(currentTime);
        return "(System time: " + time + ") ";
    }

    /**
     * Helper function to print out the current thread information
     */
    private void printThreadInfo() {
        System.out.print(timestamp() + "Active thread: " + Thread.currentThread().getName());
        System.out.println(" number " + Thread.currentThread().getId());
    }

    /**
     * Helper function to sleep the thread to show concurrency control. Helps with readability
     */
    private void sleepThread(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted");
            System.exit(1);
        }
    }
}
