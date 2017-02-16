package dbshadow.test.filewriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import dbshadow.io.ResultSetFileWriter;
import dbshadow.table.MetaData;
import dbshadow.table.MetaDataRetriever;
import dbshadow.test.SrcDest;
import org.junit.jupiter.api.Test;

import java.io.File;
//import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * @author matt
 *
 */
public class FileWriterTest extends SrcDest {
	@Test
	public void write()
	  throws SQLException, IOException {
		final String sql = "select * from test";

		srcSessionFactory.openSession().doWork(srcConn -> { 
			try {
				final MetaData meta = MetaDataRetriever.getMeta(sql, srcConn);

				// To download the entire result set it is supposedly much
				// faster to do forward only. This means that we can only
				// call rs.next() and not rs.first(). So we must make sure
				// that rs.next() is not called before we start downloading
				// the table. This is called in populateTable.
				try (PreparedStatement pstmt = srcConn.prepareStatement(sql,
						ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
					pstmt.setFetchSize(100);
					pstmt.setMaxRows(0);
					try (ResultSet rs = pstmt.executeQuery(); ) {
						assertNotNull(rs);
						File f = File.createTempFile("test", "test");
						assertEquals(0, f.length());
						f.deleteOnExit();
						new ResultSetFileWriter(f).write(rs, meta);
						assertTrue(f.length() > 0);
					}
				}
			} catch (IOException e) {
				fail(e.toString());
			}
		});
	}
}
