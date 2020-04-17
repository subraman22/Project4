
/**
 * Multithreaded coordinator for the RMIGPAServer 2 phase commit.
 * Binds to each instance of the server and communicates with TCP sockets to handle 2 phase commit message passing.
 * CS 6650 Scalable Distributed Systems
 * Spring 2020 Project 3
 * 3/25/20
 * by Rohan Subramaniam
 */


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Coordinator class that connects to RMIGPAServers and handles 2 phase commit
 */
public class Coordinator {
    boolean closed = false;
    boolean inputFromAll = false;
    List<ServerThread> serverThreads;
    List<String> roles;
    int numAcceptors;
    int numPromised;
    int numAccepted;
    String[] requestArgs;
    boolean paxosDone = false;

    /**
     * Constructor that initiates the thread and data lists
     */
    public Coordinator() {
        serverThreads = new ArrayList<>(5);
        roles = new ArrayList<>(5);
        numAcceptors = 0;
        numPromised = 0;
        numAccepted = 0;
    }

    /**
     * Coordinator main method that establishes the ServerSocket. Loops to listen for new server connections and
     * spins off a new thread to handle 2 phase commit
     * @param args unused
     */
    public static void main(String[] args) {
        Socket serverSocket;
        ServerSocket coordinatorSocket = null;
        int port = 1235;
        Coordinator coord = new Coordinator();
        try {
            coordinatorSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.out.println(timestamp() + "Error while starting coordinator serverSocket");
        }

        while(!coord.closed) {
            try {
                serverSocket = coordinatorSocket.accept();
                ServerThread thread = new ServerThread(coord, serverSocket);
                (coord.serverThreads).add(thread);
                System.out.println("Now Total clients are : "+(coord.serverThreads).size());
                (coord.roles).add("ACCEPTOR");
                coord.numAcceptors += 1;
                thread.start();
            } catch (IOException e) {
                System.out.println(timestamp() + "Error while accepting Server connection to Coordinator");
            }
        }
        try {
            coordinatorSocket.close();
        } catch (IOException e) {
            System.out.println(timestamp() + "Error while closing coordinator serverSocket");
        } catch (NullPointerException f) {
            System.out.println(timestamp() + "Error while trying to close the coordinator server.");
        }

    }

    /**
     * Timestamp of the current time to print on each line.
     * @return String version of the timestamp formatted for readability
     */
    private static String timestamp() {
        long currentTime = System.currentTimeMillis();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:ms");
        //formatted value of current Date
        String time = "" + df.format(currentTime);
        return "(System time: " + time + ") ";
    }
}

/**
 * Helper thread class that handles 2 phase commit for each new incoming server connection
 */
class ServerThread extends Thread {
    DataInputStream dataIn = null;
    String line;
    String name;
    DataOutputStream dataOut = null;
    Socket serverSocket = null;
    Coordinator coord;

    /**
     * Constructor for the new thread that connects to the specified socket
     * @param coord Coordinator handling the server pool
     * @param serverSocket Socket accepted by a new server
     */
    ServerThread(Coordinator coord, Socket serverSocket) {
        this.serverSocket = serverSocket;
        this.coord = coord;
    }

    /**
     * Thread run method that establishes the socket connection to each server and handles 2 phase commit communication
     */
    @Override
    public void run() {
        try {
            dataIn = new DataInputStream(serverSocket.getInputStream());
            dataOut = new DataOutputStream(serverSocket.getOutputStream());
            name = "Thread " + Thread.currentThread().getName();
            System.out.println(timestamp() + name + " New server thread initialized. Listening...");
        } catch (IOException e) {
            System.out.println(timestamp() + name + "IOException. Could not establish data streams");
        }

        // Loops to handle PAXOS
        boolean aborting = false;
        while (!aborting) {

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted while sleeping");
            }

            try {
                line = dataIn.readUTF();
            } catch (EOFException e) {
                System.out.println(timestamp() + "EOFException while reading line from server " + name +
                        ". Server has stopped");
                aborting = true;
                line = "closed";
            } catch (IOException e) {
                System.out.println(timestamp() + name + " IOException while reading line from server " + name);
            }

            try {
                if (line.equalsIgnoreCase("closed")) {
                    System.out.println(timestamp() + "Server closed");
                }
                if (line.equalsIgnoreCase("LEARNER")) {
                    int index = coord.serverThreads.indexOf(this);
                    coord.roles.set(index, "LEARNER");
                    coord.numAcceptors -= 1;
                    System.out.println(timestamp() + "LEARNER added. Number of ACCEPTORS now: " + coord.numAcceptors);
                }
                // Starting the PAXOS, the server will request a PREPARE message to be sent
                if (line.startsWith("REQUEST")) {
                    String[] params = line.split(" ");
                    System.out.println("REQUEST Params: " + Arrays.toString(params));
                    // Format will be ["REQUEST", "id", "[put,k,v]"]
                    coord.requestArgs = params;
                    System.out.println("Request args length: " + coord.requestArgs.length);
                    System.out.println(timestamp() + params[0] + " ID: " + params[1] + " request received from server "
                            + name + ". Initiating Paxos protocol");
                    int index = coord.serverThreads.indexOf(this);
                    coord.roles.set(index, "PROPOSER");
                    coord.numAcceptors -= 1;
                    writeToAllAcceptors("PREPARE " + params[1]);
                    continue;
                }
            } catch (IOException e) {
                System.out.println(timestamp() + name + " IOException while doing PREPARE phase");
                line = "ABORT";
            }

            try {  // After telling each server to prepare, the coordinator threads will listen for promises
                if (line.startsWith("PROMISE")) {
                    String[] params = line.split("[\\[\\]( ),]+");
                    System.out.println("PROMISE Params: " + Arrays.toString(params));
                    if (params.length < 3) { // If less than 3 params, the acceptor didn't have another acceptance
                        coord.numPromised += 1;

                        promiseMajorityCheck();  // Sends the accept message if a majority is reached
                        continue;
                    } else { // if the promise message is longer than 3, we have to take the previously accepted request
                        coord.numPromised += 1;
                        coord.requestArgs[2] = params[4];
                        System.out.println("Coord saved REQUEST args:  " + Arrays.toString(coord.requestArgs));

                        promiseMajorityCheck();
                        continue;
                    }
                }

            } catch (IOException e) {
                System.out.println(timestamp() + name + " IOException during promise phase.");
            } catch (IndexOutOfBoundsException f) {
                System.out.println(timestamp() + name + " IndexOutOfBoundsException. A server has been closed. Retrying");
            }

            try {
                if (line.startsWith("ACCEPT") && coord.numAccepted < coord.numAcceptors) {
                    // params format: ["ACCEPT", "REQUEST", "[put,k,v]"
                    coord.numAccepted += 1;
                    String[] params = line.split(" ");
                    System.out.println("ACCEPT received from server: " + name + ". Checking for majority");
                    acceptedMajorityCheck(params[2]); // writes command to proposers and learners.
                    continue;
                }
            } catch (IOException e) {
                System.out.println(timestamp() + name + " IOException during accept phase.");
            }

            if (coord.paxosDone) {
                //reset
            }
        }
        try {
            dataIn.close();
            dataOut.close();
            serverSocket.close();
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.out.println(timestamp() + name + "IOException while closing dead server thread");
        }
    }

    private void writeToAllAcceptors(String message) throws IOException {
        for (int i = 0; i < (coord.roles).size(); i++) {
            if ((coord.roles).get(i).equalsIgnoreCase("ACCEPTOR")) {
                ((coord.serverThreads).get(i)).dataOut.writeUTF(message);
            }
        }
    }

    private void writeToProposersAndLearners(String message) throws IOException {
        for (int i = 0; i < (coord.roles).size(); i++) {
            if ((coord.roles).get(i).equalsIgnoreCase("PROPOSER") ||
                    (coord.roles).get(i).equalsIgnoreCase("LEARNER")) {
                System.out.println("Writing to proposer/learner server " + (coord.serverThreads).get(i).name);
                ((coord.serverThreads).get(i)).dataOut.writeUTF(message);
            }
        }
    }

    private void promiseMajorityCheck() throws IOException {
        if (coord.numPromised >= ((coord.numAcceptors / 2) + 1)) { // if we have a majority
            System.out.println(timestamp() + coord.numPromised + " promised servers. Promise majority reached");
            writeToAllAcceptors("ACCEPT " + Arrays.toString(coord.requestArgs));
        } else {
            System.out.println(timestamp() + "Received PROMISE from server " + name + ". Waiting for majority");
        }
    }

    private void acceptedMajorityCheck(String message) throws IOException {
        if (coord.numAccepted >= ((coord.numAcceptors / 2) + 1)) { // if we have a majority
            System.out.println(timestamp() + coord.numAccepted + " accepted servers. Accepted majority reached");
            System.out.println("Majority accepted. Message sending to proposers and learners: " + message);
            writeToProposersAndLearners(message);
            writeToAllAcceptors("DONE");
            coord.paxosDone = true;
        } else {
            System.out.println(timestamp() + "Received ACCEPT from server " + name + ". Waiting for majority");
        }
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
}
