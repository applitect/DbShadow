package dbshadow;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DbShadowOpt {
    public enum WorkType {
        NONE, SYNC, CREATE, APPEND, TRUNC, ADD, DELETE
    };

    private Boolean rowCountError = false;
    private Integer partitionSize;
    private WorkType workType;
    private String srcCfgFilename;
    private String destCfgFilename;
    private String source;
    private String dest;
    private CommandLine line;
    private String sqlStmt;
    private String verbose;
    private boolean simulate;

    public DbShadowOpt getCommandLineArgs(String args[]) {
        Options opts = new Options();
        OptionGroup work = new OptionGroup();
        work.addOption(Option.builder("a").longOpt("add").desc("Fetch <SOURCE> data and add it to the <DEST> table. " +
                                                               "Can fail on duplicate keys. Does not update data.")
                             .build());
        work.addOption(Option.builder("c")
                             .longOpt("create").desc("Drops the <DEST> table, recreates it based on " +
                                                     "the structure of the fields from <SOURCE> and populates it with the <SOURCE> data.")
                             .build());
        work.addOption(Option.builder("z").longOpt("sync")
                             .desc("Fetch the <SOURCE> data and perform an upsert " +
                                   "into the <DEST> table. A comparison of <SOURCE> and <DEST> determines if the delete " +
                                   "option is run.")
                             .build());
        work.addOption(Option.builder("d").longOpt("delete")
                             .desc("Delete rows from the <SOURCE> that are no longer " + "in the <DEST>.").build());
        work.addOption(Option.builder("t")
                             .longOpt("trunc").desc("Truncate the <DEST> table leaving all structure in " +
                                                    "place, including indexes and populate <DEST> with current <SOURCE> data.")
                             .build());
        work.setRequired(true);
        opts.addOptionGroup(work);

        OptionGroup query = new OptionGroup();
        query.addOption(Option.builder("s").longOpt("source").hasArg(true)
                              .desc("The name of the <SOURCE> table to shadow. " + "May need to include the schema.").build());
        query.addOption(Option.builder("q")
                              .longOpt("sql").hasArg(true).desc("A query statement against the source database. " +
                                                                "The result field names and query table will be used to populate <DEST>.")
                              .build());
        query.setRequired(true);
        opts.addOptionGroup(query);

        OptionGroup output = new OptionGroup();
        output.addOption(Option.builder("o").longOpt("outfile").hasArg(true)
                               .desc("Store the resulting data in the named " + "output file.").build());
        output.addOption(Option.builder("d").longOpt("destConfig").hasArg(true)
                               .desc("The destination database connection" + " config file.").build());
        output.setRequired(true);
        opts.addOptionGroup(output);

        opts.addOption(Option.builder("i").longOpt("srcConfig").hasArg(true).required()
                             .desc("The source database " + "connection config file.").build());

        opts.addOption(Option.builder("n").longOpt("dest")
                             .hasArg(true).desc("Override the <DEST> table name. " +
                                                "Useful when querying a SQL statement that may not be a full table or joined tables.")
                             .build());

        opts.addOption(Option.builder("v").longOpt("verbose").hasArg().optionalArg(true).desc("Verbose output.").build());

        opts.addOption(Option.builder("r").longOpt("rowCountCheck").hasArg(false)
                             .desc("After work has been completed, " + "compare the row counts of <SOURCE> and <DEST>").build());
        opts.addOption(Option.builder("x")
                             .longOpt("simulate").hasArg(false).desc("Don't actually run any delete, " +
                                                                     "insert, or update statements but instead send statements to stdout.")
                             .build());
        opts.addOption(Option.builder("p").longOpt("partition")
                             .hasArg(true).desc("Set the partition size (the " +
                                                "number of records in a set) when comparing the primary keys between two tables.")
                             .build());
        OptionGroup usage = new OptionGroup();
        usage.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage").build());
        usage.addOption(Option.builder("u").longOpt("usage").hasArg(false).desc("Show usage").build());
        opts.addOptionGroup(usage);

        CommandLineParser parser = new DefaultParser();
        line = null;
        try {
            line = parser.parse(new Options().addOptionGroup(usage), args, true);
            if (line.hasOption("usage") || line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(120,
                                    "dwshadow [--add|--create|--sync|--delete|--trunc] [--source tablename|--sql queryStmt] " +
                                         "[--outfile filename|--destConfig configFile] " +
                                         "[--srcConfig configFile] [--destTableName tableName] {--verbose {n}}" +
                                         "{--rowCountCheck} {--simulate} {--partition size} {--help|--usage}",
                                    "", opts, "");
                System.exit(0);
            }

            line = parser.parse(opts, args);

            if (line.hasOption("verbose"))
                verbose = Optional.ofNullable(line.getOptionValue("verbose")).orElse("1");

            if (line.hasOption("simulate"))
                simulate = true;

            // rowCountError
            if (line.hasOption("rowCountError"))
                rowCountError = true;

            final List<String> errors = new ArrayList<String>();

            if (line.hasOption("partition")) {
                try {
                    partitionSize = Integer.parseInt(line.getOptionValue("partition", "100000"));
                } catch (Exception e) {
                    errors.add("ERROR: partition size must be an integer.");
                }
            }

            workType = WorkType.NONE;
            if (line.hasOption("sync"))
                workType = WorkType.SYNC;
            if (line.hasOption("create"))
                workType = WorkType.CREATE;
            if (line.hasOption("append"))
                workType = WorkType.APPEND;
            if (line.hasOption("trunc"))
                workType = WorkType.TRUNC;
            if (line.hasOption("add"))
                workType = WorkType.ADD;
            if (line.hasOption("delete"))
                workType = WorkType.DELETE;

            // Get Source configs
            source = line.getOptionValue("source");
            srcCfgFilename = line.getOptionValue("srcConfig");
            final File srcFile = new File(srcCfgFilename);
            if (!srcFile.exists())
                errors.add("srcConfig file does not exist.");

            // Get Destination configs
            dest = line.getOptionValue("dest");
            destCfgFilename = line.getOptionValue("destConfig");
            final File destFile = new File(destCfgFilename);
            if (!destFile.exists())
                errors.add("destConfig file does not exist.");

            setSqlStmt(line.getOptionValue("sql"));

            // We can't continue into reading the configs if the args weren't
            // passed in. Have to throw the errors and exit here.
            if (errors.size() > 0 || workType == WorkType.NONE) {
                for (String error : errors)
                    System.err.println(error);
                System.exit(-1);
            } else if (srcCfgFilename.equals(destCfgFilename)) {
                System.err.println("Source and destination databases cannot be the same.");
                System.exit(-1);
            }

        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("dbshadow: " + exp.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(120,
                                "dwshadow [--add|--create|--sync|--delete|--trunc] [--source tablename|--sql queryStmt] " +
                                     "[--outfile filename|--destConfig configFile] " +
                                     "[--srcConfig configFile] [--destTableName tableName] {--verbose {n}}" +
                                     "{--rowCountCheck} {--simulate} {--partition size} {--help|--usage}",
                                "", opts, "");
            System.exit(1);
        }
        return this;
    }

    public Boolean getRowCountError() {
        return rowCountError;
    }

    public void setRowCountError(Boolean rowCountError) {
        this.rowCountError = rowCountError;
    }

    public Integer getPartitionSize() {
        return partitionSize;
    }

    public void setPartitionSize(Integer partitionSize) {
        this.partitionSize = partitionSize;
    }

    public String getSrcCfgFilename() {
        return srcCfgFilename;
    }

    public void setSrcCfgFilename(String srcCfgFilename) {
        this.srcCfgFilename = srcCfgFilename;
    }

    public String getDestCfgFilename() {
        return destCfgFilename;
    }

    public void setDestCfgFilename(String destCfgFilename) {
        this.destCfgFilename = destCfgFilename;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public CommandLine getLine() {
        return line;
    }

    public void setLine(CommandLine line) {
        this.line = line;
    }

    public WorkType getWorkType() {
        return workType;
    }

    public void setWorkType(WorkType workType) {
        this.workType = workType;
    }

    public String getSqlStmt() {
        return sqlStmt;
    }

    public void setSqlStmt(String sqlStmt) {
        this.sqlStmt = sqlStmt;
    }

    public String getVerbose() {
        return verbose;
    }

    public void setVerbose(String verbose) {
        this.verbose = verbose;
    }

    public boolean simulate() {
        return simulate;
    }

    public void setSimulate(boolean simulate) {
        this.simulate = simulate;
    }

}
