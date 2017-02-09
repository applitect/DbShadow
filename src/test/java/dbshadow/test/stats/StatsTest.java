package dbshadow.test.stats;

import java.io.IOException;
import java.sql.SQLException;

import dbshadow.stats.StatCollector;
import dbshadow.table.TableWriter;

/**
 * @author matt
 *
 */
//public class StatsTest extends SrcDest {
//	boolean initialFetchCompleted = false;
//	@Test
//	public void initialFetchTime()
//	  throws SQLException, IOException {
//		StatCollector c = new StatCollector() {
//			@Override
//			public void completeInitialFetch() {
//				initialFetchCompleted = true;
//			}
//		};
//
//		TableWriter writer = new TableWriter("select * from test", srcConn, c);
//		writer.writeDataCreate("test2", destConn);
//
//		assert initialFetchCompleted;
//		assert c.getInitialFetchTime() > -1L;
//	}
//}
