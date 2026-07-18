package com.example.demo;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class YClient {
    static final String SERVER_IP = "127.0.0.1";
    static final int SERVER_PORT = 9000;

    static String myId;
    static int myPort;
    static Socket serverSocket;
    static PrintWriter serverWriter;
    static BufferedReader serverReader;

    static String lastList = "";
    static String peerIp = "";
    static int peerPort = 0;
    static boolean peerReady = false;
    static boolean running = true;

    public static void main(String[] args) {
        try {
            Scanner input = new Scanner(System.in);

            System.out.println("Enter your user ID:");
            myId = input.nextLine();

            ServerSocket myReceiver = new ServerSocket(0);
            myPort = myReceiver.getLocalPort();

            serverSocket = new Socket(SERVER_IP, SERVER_PORT);
            serverWriter = new PrintWriter(serverSocket.getOutputStream(), true);
            serverReader = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));

            System.out.println("Connected to Hadoop directory server");
            System.out.println("My receiving port is " + myPort);

            Thread aliveThread = new Thread(new Runnable() {
                public void run() {
                    sendAlive();
                }
            });
            aliveThread.start();

            Thread serverThread = new Thread(new Runnable() {
                public void run() {
                    readFromServer();
                }
            });
            serverThread.start();

            Thread receiveThread = new Thread(new Runnable() {
                public void run() {
                    receiveMessages(myReceiver);
                }
            });
            receiveThread.start();

            menu(input);

            running = false;
            serverWriter.println("DISCONNECT|" + myId);
            serverSocket.close();
            myReceiver.close();
        } catch (Exception e) {
            System.out.println("Client stopped");
        }
    }

    static void menu(Scanner input) {
        while (running) {
            System.out.println("");
            System.out.println("Commands:");
            System.out.println("list");
            System.out.println("msg userId message");
            System.out.println("quit");
            System.out.println("Enter command:");

            String command = input.nextLine();

            if (command.equals("list")) {
                serverWriter.println("GET_LIST");
                sleep(300);
                System.out.println("Online users: " + lastList);
            } else if (command.startsWith("msg ")) {
                sendMessage(command);
            } else if (command.equals("quit")) {
                break;
            } else {
                System.out.println("Wrong command");
            }
        }
    }

    static void sendMessage(String command) {
        String[] parts = command.split(" ", 3);

        if (parts.length < 3) {
            System.out.println("Use: msg userId message");
            return;
        }

        String toId = parts[1];
        String message = parts[2];

        peerReady = false;
        serverWriter.println("GET_PEER|" + toId);

        int tries = 0;

        while (!peerReady && tries < 10) {
            sleep(300);
            tries++;
        }

        if (!peerReady) {
            System.out.println("User was not found");
            return;
        }

        try {
            Socket peerSocket = new Socket(peerIp, peerPort);
            PrintWriter writer = new PrintWriter(peerSocket.getOutputStream(), true);
            writer.println("CHAT|" + myId + "|" + message);
            peerSocket.close();
            System.out.println("Message sent");
        } catch (Exception e) {
            System.out.println("Could not send message");
        }
    }

    static void sendAlive() {
        while (running) {
            serverWriter.println("ALIVE|" + myId + "|" + myPort);
            sleep(1000);
        }
    }

    static void readFromServer() {
        try {
            while (running) {
                String line = serverReader.readLine();

                if (line == null) {
                    break;
                }

                String[] parts = line.split("\\|");

                if (parts[0].equals("LIST")) {
                    if (parts.length > 1) {
                        lastList = parts[1];
                    } else {
                        lastList = "";
                    }
                } else if (parts[0].equals("PEER") && parts.length >= 4) {
                    peerIp = parts[2];
                    peerPort = Integer.parseInt(parts[3]);
                    peerReady = true;
                } else if (parts[0].equals("NOT_FOUND")) {
                    peerReady = false;
                }
            }
        } catch (Exception e) {
            System.out.println("Server connection closed");
        }
    }

    static void receiveMessages(ServerSocket myReceiver) {
        try {
            while (running) {
                Socket peerSocket = myReceiver.accept();
                BufferedReader reader = new BufferedReader(new InputStreamReader(peerSocket.getInputStream()));
                String line = reader.readLine();

                if (line != null) {
                    String[] parts = line.split("\\|", 3);

                    if (parts.length == 3 && parts[0].equals("CHAT")) {
                        System.out.println("");
                        System.out.println(parts[1] + ": " + parts[2]);
                    }
                }

                peerSocket.close();
            }
        } catch (Exception e) {
            System.out.println("Receive stopped");
        }
    }

    static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
            System.out.println("Sleep error");
        }
    }
}
