package dbshadow.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dbshadow.DbShadowOpt;
import dbshadow.exception.IncompleteArgumentException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

@RunWith(JUnitPlatform.class)
public class CommandLineOptsTest {
    @Test
    @DisplayName("Look for too few of args")
    void simpleTest() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DbShadowOpt().getCommandLineArgs(new String[] {"--src"});
        });
    }

    @Test
    @DisplayName("Make sure file existance check works.")
    void buildupTest() {
        assertThrows(IOException.class, () -> {
            new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--source", "table", "--dest", "table", "--destConfig", "blah", "--srcConfig", "blah", "--destConfig", "blah2"});
        });
    }

    @Test
    @DisplayName("Make sure a valid set of args work.")
    void goodArgsTest() throws IncompleteArgumentException, IOException {
        // Build two files for configs to test
        File f = null, f2 = null;
        String path = System.getProperty("java.io.tmpdir");
        try {
            f = new File(path + "srcConfig");
            f.createNewFile();
            f2 = new File(path + "destConfig");
            f2.createNewFile();

            new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            new DbShadowOpt().getCommandLineArgs(new String[] {"--create", "--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            new DbShadowOpt().getCommandLineArgs(new String[] {"--trunc", "--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--sql", "select * from blah", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            // TODO once delete is brought back make sure the following works.
//            new DbShadowOpt().getCommandLineArgs(new String[] {"--delete", "--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});

            // Dest does not need to exist. If it doesn't exist, use the same name as source.
            DbShadowOpt opts = new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--source", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            assertEquals(opts.getDest(), opts.getSource());
        } finally {
            if (f != null)
                f.delete();
            if (f2 != null)
                f2.delete();
        }
    }

    @Test
    @DisplayName("Make sure we check for valid sql syntax.")
    void goodSqlTest() throws IncompleteArgumentException, IOException {
        // Build two files for configs to test
        File f = null, f2 = null;
        String path = System.getProperty("java.io.tmpdir");
        try {
            f = new File(path + "srcConfig");
            f.createNewFile();
            f2 = new File(path + "destConfig");
            f2.createNewFile();
            // Check good line with sql statement
            new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--sql", "select * from blah", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            // Check to make sure dest table is set with sql statement
            assertThrows(IncompleteArgumentException.class, () -> {
            new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--sql", "select * from blah", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            });
            // Check for valid sql
            assertThrows(IllegalArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--sql", "not a valid sql statement", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            });
        } finally {
            if (f != null)
                f.delete();
            if (f2 != null)
                f2.delete();
        }
    }

    @Test
    @DisplayName("Check for outfile mutual exclivity.")
    void outfileParamCheck() throws IncompleteArgumentException, IOException {
        // Build two files for configs to test
        File f = null, f2 = null;
        String path = System.getProperty("java.io.tmpdir");
        try {
            f = new File(path + "srcConfig");
            f.createNewFile();
            f2 = new File(path + "destConfig");
            f2.createNewFile();

            new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--source", "blah", "--dest", "blah", "--srcConfig", path + "srcConfig", "--outfile", "out.txt",});

            assertThrows(IllegalArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--source", "blah", "--dest", "table", "--outfile", "out.txt", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            });

            // Make sure we don't overwrite existing file
            assertThrows(IOException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--add", "--source", "blah", "--srcConfig", path + "srcConfig", "--outfile", path + "srcConfig"});
            });

        } finally {
            if (f != null)
                f.delete();
            if (f2 != null)
                f2.delete();
        }
    }

    @Test
    @DisplayName("Make sure we catch missing args.")
    void missingArgsTest() throws IncompleteArgumentException, IOException {
        // Build two files for configs to test
        File f = null, f2 = null;
        String path = System.getProperty("java.io.tmpdir");
        try {
            f = new File(path + "srcConfig");
            f.createNewFile();
            f2 = new File(path + "destConfig");
            f2.createNewFile();
            assertThrows(IllegalArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            });
            assertThrows(IllegalArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--dest", "table", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            });
            assertThrows(IllegalArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--source", "table", "--dest", "table", "--destConfig", path + "destConfig"});
            });
            assertThrows(IncompleteArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--source", "table", "--dest", "table", "--srcConfig", path + "srcConfig"});
            });
            assertThrows(IllegalArgumentException.class, () -> {
                new DbShadowOpt().getCommandLineArgs(new String[] {"--sync", "--source", "table", "--dest", "--srcConfig", path + "srcConfig", "--destConfig", path + "destConfig"});
            });
        } finally {
            if (f != null)
                f.delete();
            if (f2 != null)
                f2.delete();
        }
    }
    
    // TODO need to test optional args.
}
