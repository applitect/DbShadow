package dbshadow.test.filewriter;

//import java.io.File;
//import java.io.IOException;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;

//
///**
// * @author matt
// *
// */
//public class FileWriterTest extends SrcDest {
//	@Test
//	public void write()
//	  throws SQLException, IOException {
//		final String sql = "select * from test";
//
//		final MetaData meta = MetaDataRetriever.getMeta(sql, srcConn);
//
//		// To download the entire result set it is supposedly much
//		// faster to do forward only. This means that we can only
//		// call rs.next() and not rs.first(). So we must make sure
//		// that rs.next() is not called before we start downloading
//		// the table. This is called in populateTable.
//		PreparedStatement pstmt = srcConn.prepareStatement(sql,
//				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//		pstmt.setFetchSize(100);
//		pstmt.setMaxRows(0);
//		addCloseMe(pstmt);
//
//		ResultSet rs = pstmt.executeQuery();
//		addCloseMe(rs);
//
//		assert rs != null;
//
//		File f = File.createTempFile("test", "test");
//		assert f.length() == 0;
//		f.deleteOnExit();
//		new ResultSetFileWriter(f).write(rs, meta);
//		assert f.length() > 0;
//	}
//}
