package com.example.demo;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class YServer {
    static final int SERVER_PORT = 9000;
    static final String HDFS_FOLDER = "/phase2";
    static final String HDFS_FILE = HDFS_FOLDER + "/clients.txt";
    static final String LOCAL_FILE = "clients.txt";
    static final long TIMEOUT = 5000;

    public static void main(String[] args) {
        prepareStorage();

        try {
            ServerSocket server = new ServerSocket(SERVER_PORT);
            System.out.println("Hadoop directory server started on port " + SERVER_PORT);

            while (true) {
                Socket socket = server.accept();

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        serveClient(socket);
                    }
                });

                t.start();
            }
        } catch (Exception e) {
            System.out.println("Server stopped");
        }
    }

    static void serveClient(Socket socket) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            String ip = socket.getInetAddress().getHostAddress();

            while (true) {
                String line = reader.readLine();

                if (line == null) {
                    break;
                }

                String[] p = line.split("\\|");

                if (p[0].equals("ALIVE") && p.length >= 3) {
                    String id = p[1];
                    int port = Integer.parseInt(p[2]);
                    saveClient(id, ip, port);
                    writer.println("OK");
                } else if (p[0].equals("GET_LIST")) {
                    writer.println(makeList());
                } else if (p[0].equals("GET_PEER") && p.length >= 2) {
                    ClientInfo client = findClient(p[1]);

                    if (client == null) {
                        writer.println("NOT_FOUND");
                    } else {
                        writer.println("PEER|" + client.id + "|" + client.ip + "|" + client.port);
                    }
                } else if (p[0].equals("DISCONNECT") && p.length >= 2) {
                    removeClient(p[1]);
                    writer.println("BYE");
                    break;
                } else {
                    writer.println("BAD_COMMAND");
                }
            }

            socket.close();
        } catch (Exception e) {
            System.out.println("Client connection closed");
        }
    }

    static synchronized void saveClient(String id, String ip, int port) {
        ArrayList<ClientInfo> clients = loadClients();
        boolean found = false;

        for (int i = 0; i < clients.size(); i++) {
            ClientInfo client = clients.get(i);

            if (client.id.equals(id)) {
                client.ip = ip;
                client.port = port;
                client.lastSeen = System.currentTimeMillis();
                found = true;
            }
        }

        if (!found) {
            clients.add(new ClientInfo(id, ip, port, System.currentTimeMillis()));
            System.out.println(id + " joined");
        }

        saveClients(clients);
    }

    static synchronized void removeClient(String id) {
        ArrayList<ClientInfo> clients = loadClients();

        for (int i = 0; i < clients.size(); i++) {
            if (clients.get(i).id.equals(id)) {
                clients.remove(i);
                System.out.println(id + " left");
                break;
            }
        }

        saveClients(clients);
    }

    static synchronized ClientInfo findClient(String id) {
        ArrayList<ClientInfo> clients = loadClients();

        for (int i = 0; i < clients.size(); i++) {
            if (clients.get(i).id.equals(id)) {
                return clients.get(i);
            }
        }

        return null;
    }

    static synchronized String makeList() {
        ArrayList<ClientInfo> clients = loadClients();
        String list = "";

        for (int i = 0; i < clients.size(); i++) {
            list = list + clients.get(i).id;

            if (i < clients.size() - 1) {
                list = list + ",";
            }
        }

        return "LIST|" + list;
    }

    static ArrayList<ClientInfo> loadClients() {
        getFileFromHadoop();
        ArrayList<ClientInfo> clients = new ArrayList<ClientInfo>();
        long now = System.currentTimeMillis();

        try {
            File file = new File(LOCAL_FILE);

            if (!file.exists()) {
                return clients;
            }

            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();

            while (line != null) {
                String[] p = line.split("\\|");

                if (p.length == 4) {
                    ClientInfo client = new ClientInfo(p[0], p[1], Integer.parseInt(p[2]), Long.parseLong(p[3]));

                    if (now - client.lastSeen <= TIMEOUT) {
                        clients.add(client);
                    }
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (Exception e) {
            System.out.println("Could not read clients file");
        }

        return clients;
    }

    static void saveClients(ArrayList<ClientInfo> clients) {
        try {
            PrintWriter writer = new PrintWriter(new FileWriter(LOCAL_FILE));

            for (int i = 0; i < clients.size(); i++) {
                ClientInfo client = clients.get(i);
                writer.println(client.id + "|" + client.ip + "|" + client.port + "|" + client.lastSeen);
            }

            writer.close();
            putFileInHadoop();
        } catch (Exception e) {
            System.out.println("Could not save clients file");
        }
    }

    static void prepareStorage() {
        runCommand("hdfs dfs -mkdir -p " + HDFS_FOLDER);

        File file = new File(LOCAL_FILE);

        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (Exception e) {
            System.out.println("Could not create local clients file");
        }

        putFileInHadoop();
    }

    static void getFileFromHadoop() {
        runCommand("hdfs dfs -get -f " + HDFS_FILE + " " + LOCAL_FILE);
    }

    static void putFileInHadoop() {
        runCommand("hdfs dfs -put -f " + LOCAL_FILE + " " + HDFS_FILE);
    }

    static boolean runCommand(String command) {
        try {
            Process process;
            String os = System.getProperty("os.name").toLowerCase();

            if (os.contains("win")) {
                process = new ProcessBuilder("cmd", "/c", command).start();
            } else {
                process = new ProcessBuilder("sh", "-c", command).start();
            }

            process.waitFor();
            return process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    static class ClientInfo {
        String id;
        String ip;
        int port;
        long lastSeen;

        ClientInfo(String id, String ip, int port, long lastSeen) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.lastSeen = lastSeen;
        }
    }
}
