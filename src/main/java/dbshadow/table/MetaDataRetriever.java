package dbshadow.table;

import dbshadow.log4j.XLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * MetaDataRetriever creates {@link MetaData}.
 * @author matt
 *
 */
public class MetaDataRetriever {
    private static final Logger log = LogManager.getLogger(MetaDataRetriever.class);

    /**
     * Creates a new {@link MetaData} object for a given sql query.
     * At this time only one table in the from clause can be specified
     * due to a Oracle inefficiency in their JDBC driver meta data fetch.
     * @param sql
     * @param sourceConn
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public static MetaData getMeta(String sql, Connection sourceConn)
      throws SQLException, IOException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            // To download the entire result set it is supposedly much
            // faster to do forward only. This means that we can only
            // call rs.next() and not rs.first(). So we must make sure
            // that rs.next() is not called before we start downloading
            // the table. This is called in populateTable.
            pstmt = sourceConn.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            pstmt.setFetchSize(1);
            pstmt.setMaxRows(1);
            rs = pstmt.executeQuery();

            // If no data was found quit.
            if (rs == null) {
                log.warn("No data found in <SOURCE> using sql statement:" + sql);
                return null;
            }
            log.log(XLevel.DEBUG2, "Processing <SOURCE> data...");

            // Get the table's metadata
            ResultSetMetaData meta = rs.getMetaData();

            // For the result metadata set, we need to cache the count and
            // column names. Calls to getXXX for meta data can make calls
            // back to the database. We're pretty sure they're not going to
            // change while were running.
            int cols = meta.getColumnCount() + 1;
            ArrayList<String> sourceCols = new ArrayList<String>();
            for (int i = 1; i < cols; i++)
                sourceCols.add(meta.getColumnName(i));

            // XXX Oracle 10 Sucks!
            // Oracle does not implement the getTableName and getSchemaName
            // methods. Since they do not implement them, we have to parse
            // the query to determine the table name.
            String tableName = meta.getTableName(1);
            if (tableName.trim().length() == 0) {
                Pattern p = Pattern.compile(".*\\s+from\\s+\\w*?\\.?(\\w+).*",
                        Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(sql);
                if (m.matches())
                    tableName = m.group(1).toUpperCase();
            }

            MetaData metaData = new MetaData(tableName, meta, sourceConn.getMetaData());
            return metaData;
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();
            } catch (SQLException ignored) {
            }
        }
    }
}
