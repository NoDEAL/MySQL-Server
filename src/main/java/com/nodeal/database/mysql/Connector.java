package com.nodeal.database.mysql;

import com.nodeal.database.mysql.structor.Query;
import com.nodeal.database.mysql.structor.QueryResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.TimeZone;

/**
 * Created by 김지환 on 2016-12-03.
 */
public class Connector {
    private ArrayList<Query> mQueryList;
    private ArrayList<QueryResult> mResultList;
    private final Connection mConnection;

    public Connector(String url, String id, String password) throws SQLException {
        mQueryList = new ArrayList<>();
        mResultList = new ArrayList<>();
        mConnection = getConnection(url, id, password);
    }

    private Connection getConnection(String url, String id, String password) throws SQLException {
        try {
            TimeZone timeZone = TimeZone.getTimeZone("KST");
            TimeZone.setDefault(timeZone);
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            return DriverManager.getConnection("jdbc:mysql://" + url + ":3306", id, password);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void query(Query query){
        mQueryList.add(query);
    }

    public int makeId() {
        boolean isUnique = false;
        Random random = new Random();

        int id = 0;
        makeRandom:
        while (!isUnique) {
            id = random.nextInt();
            for (Query query : mQueryList) {
                if (id == query.queryId) {
                    isUnique = false;
                    continue makeRandom;
                } else isUnique = true;
            }
        }

        return id;
    }

    public QueryResult getResult(int id) {
        for (int i = 0; i < mResultList.size(); i++) {
            if (mResultList.get(i).queryId == id) {
                QueryResult queryResult = mResultList.get(i);
                mResultList.remove(i);

                return queryResult;
            }
        }

        return null;
    }

    public class QueryThread extends Thread {
        public final int THREAD_STOP = -1;
        public final int THREAD_RUN = 1;
        private int mStatus = THREAD_RUN;
        private Statement mStatement;
        private ResultSet mResultSet;

        public QueryThread() {
            try {
                mStatement = mConnection.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public void letStop() {
            mStatus = THREAD_STOP;
        }

        private long mDelay = 1000;
        private int mDelayCount = 1;

        @Override
        public void run() {
            if (mConnection == null || mStatement == null) {
                mStatus = THREAD_STOP;
            }

            while (mStatus == THREAD_RUN) {
                if (mQueryList.size() == 0) {
                    try {
                        Thread.sleep(mDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (mDelayCount < 10) mDelayCount++;
                    else {
                        mDelay += 1000;
                        mDelayCount = 0;
                    }

                    continue;
                } else {
                    mDelay = 1000;
                    mDelayCount = 0;
                }

                Query query = mQueryList.get(0);

                try {
                    QueryResult queryResult = new QueryResult(query.getAddress(), query.queryId);

                    if (query.needResult) {
                        mResultSet = mStatement.executeQuery(query.query);
                        queryResult.result = QueryResult.makeJson(mResultSet, query.columns);
                    } else {
                        int result = mStatement.executeUpdate(query.query);
                        queryResult.result = QueryResult.makeJson(result);
                    }

                    mResultList.add(queryResult);
                    mQueryList.remove(0);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
