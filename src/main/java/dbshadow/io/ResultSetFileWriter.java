package dbshadow.io;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import dbshadow.table.MetaData;
import dbshadow.table.TableColumn;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResultSetFileWriter {
    private static final Logger log = LogManager.getLogger(ResultSetFileWriter.class);

    private static final String DELIMETER = "|";
    private static final String LINEFEED = "\r\n";

    private File file;

    public ResultSetFileWriter(String filename) {
        this.file = new File(filename);
    }

    public ResultSetFileWriter(File file) {
        this.file = file;
    }

    public void write(final ResultSet rs, final MetaData meta)
    throws IOException, SQLException {
        FileWriter fw = null;
        try {
            fw = new FileWriter(file);

            log.info("Writing table data to file...");
            // Print column names at the top of the file.
            for (TableColumn col : meta.getColumns())
                fw.write(col.getName() + DELIMETER);
            fw.write(LINEFEED);

            // Add each record to the file.
            if (rs.first()) {
	            do {
	                for (TableColumn col : meta.getColumns()) {
	                    fw.write(rs.getObject(col.getName()) + DELIMETER);
	                    log.info(rs.getObject(col.getName()) + DELIMETER);
	                }
	                fw.write(LINEFEED);
	            } while (rs.next());
	            log.info("Finished Writing file.");
            }
        } finally {
            try {
                if (rs != null) rs.close();
                try {
                    if (fw != null) fw.close();
                } catch (IOException e) {
                    log.error(e, e.fillInStackTrace());
                }
            } catch (SQLException ignored) {
            }
        }
    }
}
