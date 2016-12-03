package com.nodeal.database.mysql.structor;

/**
 * Created by 김지환 on 2016-12-03.
 */
public class Query {
    private final String[] NO_RESULT = { "insert|update" };
    private final String[] NEED_RESULT = { "show|select" };

    private final String mAddress;

    public final String query;
    public final boolean needResult;
    public String[] columns;

    public final int queryId;

    public Query(String address, String query, int queryId) {
        if (needResult(query.split(" ")[0])) {
            needResult = true;
        } else needResult = false;

        this.query = query;
        this.queryId = queryId;
        mAddress = address;
        columns = null;
    }

    public void setColumn(String... columns) {
        this.columns = columns;
    }

    private boolean needResult(String command) {
        for (String noResult : NO_RESULT) {
            if (noResult.toUpperCase().equals(command.toUpperCase())) return false;
        }

        return true;
    }

    public String getAddress() { return mAddress; }
}
