package com.nodeal.database.mysql;

import com.nodeal.database.mysql.structor.Query;
import com.nodeal.database.mysql.structor.QueryResult;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;

/**
 * Created by 김지환 on 2016-12-03.
 */
public class Server {
    private static final int SERVER_PORT = 5000;

    public static void main(String... args) throws SQLException {
        ServerSocket serverSocket = null;
        Connector connector = null;
        try {
            connector = new Connector("URL", "Database","ID","PASSWORD");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        if (connector == null) {
            System.err.println("MySQL connection failed! Exiting...");
            System.exit(0);
        } else System.out.println(connector.getConnection());

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
                InetAddress address = socket.getInetAddress();
                System.out.println("From: " + address);

                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

                String receivedQuery = dataInputStream.readUTF();
                JSONObject jsonObject = new JSONObject(receivedQuery);
                Query query = new Query(address.toString(), jsonObject.getString("query"), connector.makeId());
                query.columns = jsonToColumn(jsonObject);

                dataOutputStream.writeUTF("ID:" + query.queryId);

                connector.query(query);

                QueryResult queryResult = connector.getResult(query.queryId);
                while (queryResult == null) {
                    Thread.sleep(1000);
                    queryResult = connector.getResult(query.queryId);
                }

                dataOutputStream.writeUTF(queryResult.result.toString());

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

    public static String[] jsonToColumn(JSONObject jsonObject) {
        String[] columns = new String[jsonObject.length() - 1];

        for (int i = 1; i < jsonObject.length(); i++) {
            columns[i - 1] = (String) jsonObject.get("column" + i);
        }

        return columns;
    }
}
