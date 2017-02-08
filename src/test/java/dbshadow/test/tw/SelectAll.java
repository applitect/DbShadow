package dbshadow.test.tw;
//
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.List;
//
//import org.hibernate.HibernateException;
//import org.hibernate.Session;
//import dbshadow.sync.DataSync;
//import dbshadow.table.MetaData;
//import dbshadow.table.MetaDataRetriever;
//
//public class SelectAll {
// TODO: switch to using doWork.
//    private String table;
//    private Connection c;
//
//    public SelectAll(String table, Connection c) {
//        this.table = table;
//        this.c = c;
//    }
//
//    public Object run(Session s) {
//        @SuppressWarnings("unchecked")
//        List<Object[]> l;
//        try {
//            MetaData meta = MetaDataRetriever.getMeta("select * from " + table, c);
//            StringBuilder sql = new StringBuilder();
//            sql.append(DataSync.genSelWhereStmt(
//                table, meta, false));
//            sql.append("ORDER BY ");
//            for (String key : meta.getPkeys())
//                sql.append(key).append(",");
//            char ch = sql.charAt(sql.length() - 1);
//            if (ch == ',')
//                sql.deleteCharAt(sql.length() - 1);
//
//            l = s.createSQLQuery(sql.toString()).list();
//            assert l != null;
//            //assert l.size() > 0;
//            return l;
//        } catch (HibernateException e) {
//        } catch (SQLException e) {
//        } catch (IOException e) {
//        }
//        assert false : "Query failed";
//        return null;
//    }
//}
