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
import java.io.FileInputStream;
//import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * @author matt
 *
 */
public class FileWriterTest extends SrcDest {
	private static String data = "ID|NAME|REC_DATE|REC_TIME|CREATED_DT|NUM_TIMES|\n" + 
			"10|Bruce Wayne|2012-07-01|10:50:36|2012-07-23 10:51:01.435|4|\n" + 
			"20|Clark Kent|2012-07-01|10:50:36|2012-07-23 10:51:01.435|1|\n" + 
			"30|Steve Austin|2012-07-01|10:50:36|2012-07-23 10:51:01.435|1|\n" + 
			"40|Steven Rogers|2012-07-01|10:50:36|2012-07-23 10:51:01.435|0|\n" + 
			"50|Bruce Banner|2012-07-01|10:50:36|2012-07-23 10:51:01.435|7|\n" + 
			"60|Linda Danvers|2012-07-01|10:50:36|2012-07-23 10:51:01.435|3|\n" + 
			"70|Benjamin Grimm|2012-07-01|10:50:36|2012-07-23 10:51:01.435|2|\n" + 
			"80|James Logan Howlett|2012-07-01|10:50:36|2012-07-23 10:51:01.435|1|";

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
						// Now read f and make sure it matches what we expect
						FileInputStream fis = new FileInputStream(f);
						long len = f.length() + 1;
						byte[] b = new byte[(int) len];
						fis.read(b);
						String str = new String(b, "UTF-8");
						assertEquals(data, str);
					}
				}
			} catch (IOException e) {
				fail(e.toString());
			}
		});
	}
}
