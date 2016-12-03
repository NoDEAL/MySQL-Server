package com.nodeal.database.mysql.structor;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by 김지환 on 2016-12-03.
 */
public class QueryResult {
    public JSONArray result;
    private final String mAddress;
    public final int queryId;

    public QueryResult(String address, int queryId) {
        mAddress = address;
        this.queryId = queryId;
    }

    public String getAddress() { return mAddress; }

    public static JSONArray makeJson(ResultSet resultSet, String[] columns) throws SQLException {
        JSONArray jsonArray = new JSONArray();

        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();

            for (String column : columns) {
                jsonObject.put(column, resultSet.getObject(column));
            }

            jsonArray.put(jsonObject);
        }

        return jsonArray;
    }

    public static JSONArray makeJson(int result) {
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("result", result);
        jsonArray.put(jsonObject);

        return jsonArray;
    }
}
