package com.nodeal.database.mysql;

import com.nodeal.database.mysql.structor.Query;
import com.nodeal.database.mysql.structor.QueryResult;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by 김지환 on 2016-12-03.
 */
public class Server {
    private static final int SERVER_PORT = 5000;

    public static void main(String... args) {
        ServerSocket serverSocket = null;
        Connector connector = new Connector("localhost", "root","kimju888");

        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            System.out.println("Server opened");
        } catch (IOException e) {
            e.printStackTrace();

            return;
        }

        while (true) {
            try {
                System.out.println("Waiting...");
                Socket socket = serverSocket.accept();

                System.out.println("From: " + socket.getInetAddress());

                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                Query query = new Query(socket.getInetAddress().toString(), dataInputStream.readUTF(), connector.makeId());
                connector.query(query);

                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF("ID:" + query.queryId);

                QueryResult queryResult = connector.getResult(query.queryId);
                while (queryResult == null) {
                    Thread.sleep(1000);
                    queryResult = connector.getResult(query.queryId);
                }

                dataOutputStream.writeUTF("Result:" + queryResult.result.toString());

                socket.close();
                dataInputStream.close();
                dataOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
