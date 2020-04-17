package src;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface GPARepo extends Remote {
    String put(int key, float val) throws RemoteException;
    String get(int key) throws RemoteException;
    String delete(int key) throws RemoteException;
}
