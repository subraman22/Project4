
/**
 * Single instance of the client for the src.RMIGPAServer. Supports int Student ID keys and float GPA values.
 * Operations are Put(key, value), Get(key), and Delete(key)
 * CS 6650 Scalable Distributed Systems
 * Spring 2020 Project 3
 * 3/25/20
 * by Rohan Subramaniam
 */

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class PAXOSGPAClient {

    private static boolean autofill = true;

    /**
     * Helper function to take user input while the client is running. Deciphers the command and calls the correct
     * RMI server method
     * @param keyboard Scanner user keyboard
     * @param server src.GPARepo GPA hashmap server
     * @return boolean to modify running. False breaks the loop and exits the client
     */
    private static boolean takeInput(Scanner keyboard, src.GPARepo server) {
        // Take input
        System.out.print(timestamp() + "Enter text: ");
        String message = keyboard.nextLine();

        String response;
        if (message.equalsIgnoreCase("exit")) {
            return false;
        }
        String[] params = message.split("[( ),]+");  // Split at any combo of parenthesis, comma, or space

        // Check for the command type and executes the appropriate one
        if (params[0].equalsIgnoreCase("put") && params.length == 3) {  // Check for put and 2 args
            try {
                int key = Integer.parseInt(params[1]);
                float val = Float.parseFloat(params[2]);
                response = server.put(key, val);
            } catch (NumberFormatException e) {
                response = "Key must be int. Value must be float";
            } catch (RemoteException f) {
                response = "Remote exception while trying to put. Server unavailable.";
                System.exit(1);
            }
        } else if (params[0].equalsIgnoreCase("get") && params.length == 2) {  // Check for get and 1 arg
            try {
                int key = Integer.parseInt(params[1]);
                response = server.get(key);
            } catch (NullPointerException e) {
                response = "Key not found";
            } catch (NumberFormatException e) {
                response = "Key must be an int";
            } catch (RemoteException f) {
                response = "Remote exception while trying to get. Server unavailable.";
                System.exit(1);
            }
        } else if (params[0].equalsIgnoreCase("delete") && params.length == 2) {  // Check for delete 1 arg
            try {
                response = server.delete(Integer.parseInt(params[1]));
            } catch (NumberFormatException e) {
                response = "Key must be an int";
            } catch (RemoteException f) {
                response = "Remote exception while trying to delete. Server unavailable.";
                System.exit(1);
            }
        } else {
            response = "Invalid command. Command must be PUT(Key, Val), GET(Key), or DELETE(Key)";
        }
        System.out.println(timestamp() + response);
        return true;
    }

    /**
     * Populates the server before taking user input to have some key/value pairs readily accessible
     * @param server src.GPARepo hashmap server
     * @throws RemoteException If the server can't be reached
     */
    private static void populateServer(src.GPARepo server) throws RemoteException, InterruptedException {
        server.put(1000, 3.86f);
        System.out.println("Successfully put Key/ID: 1000  Value/GPA: 3.86");
        Thread.sleep(3000);

        server.put(1001, 2.98f);
        System.out.println("Successfully put Key/ID: 1001  Value/GPA: 2.98");
        Thread.sleep(3000);

        server.put(1002, 1.70f);
        System.out.println("Successfully put Key/ID: 1002  Value/GPA: 1.70");
        Thread.sleep(3000);

        server.put(1003, 1.22f);
        System.out.println("Successfully put Key/ID: 1003  Value/GPA: 1.22");
        Thread.sleep(3000);

        server.put(1004, 2.60f);
        System.out.println("Successfully put Key/ID: 1004  Value/GPA: 2.60");
        Thread.sleep(3000);

        server.put(1005, 3.27f);
        System.out.println("Successfully put Key/ID: 1005  Value/GPA: 3.27");
    }

    /**
     * Main method to connect to the host and port and then take user input to send commands to the server via RMI
     * @param args optional host and port arguments
     */
    public static void main(String[] args) {
        int port = 2022;
        String host = "localhost";
        // Check if the user specified
        if (args.length == 0) {
            System.out.println("Using default localhost and port 8080");
            System.out.println("To specify port and host use java -jar src.RMIGPAClient.jar <hostname> <port>");
            System.out.println("Type \"exit\" to stop the client");
        } else if (args.length < 2 || args.length > 3) {
            System.out.println("Invalid number of arguments");
            System.out.println("To specify port and host use java -jar src.RMIGPAClient.jar <hostname> <port> ");
            System.exit(1);
        } else {
            try {
                if (args.length == 3) {
                    autofill = !args[2].equalsIgnoreCase("no");
                }
                port = Integer.parseInt(args[1]);
                host = args[0];
                System.out.println("Type \"exit\" to stop the client");
            } catch (NumberFormatException e) {
                System.out.println("Invalid port argument. Port number must be an int");
                System.exit(1);
            }
        }

        try {
            // Try to bind to the RMI registry
            src.GPARepo server = (src.GPARepo) Naming.lookup("rmi://" + host + ":" + port + "/GPAService");

            if (autofill) {
                populateServer(server); // Try to send the initial puts
            }

            Scanner keyboard = new Scanner(System.in);
            boolean running = true;
            while (running) {
                running = takeInput(keyboard, server); // returns false when user inputs "exit"
            }
        } catch (RemoteException e) {
            System.out.println("Remote Exception occurred while connecting to server");
        } catch (MalformedURLException f) {
            System.out.println("Malformed URL Exception occurred while connecting to specified host");
        } catch (Exception g) {
            System.out.println("Not Bound Exception occurred while connecting to server");
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
