package com.dionoid.solr;

import javax.xml.stream.XMLInputFactory;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

/**
 * A simple utility class for posting (zipped) xml updates to a SolrCloud cluster, 
 * has a main method so it can be run on the command line.
 */
public class CloudPostTool {
  private static final String DEFAULT_ZK_HOST = "localhost:9983";
  private static final String DEFAULT_ROUTE_FIELD = "id";
  private static final int DEFAULT_NUMBER_OF_THREADS = 1;
  private static final int DEFAULT_DELAY = 0;
  private static final String DEFAULT_COMMIT = "yes";
  private static final String DEFAULT_OPTIMIZE = "no";
  private static final String DEFAULT_FILE_TYPES = "xml,zip,gz";
  private static final int DEFAULT_MULTIDOC_BATCH_SIZE = 5000; //to lower memory requirements, post multidoc-files in batches of 5k documents
  private static final int DEFAULT_COMMIT_WITHIN_SECONDS = 120; //commit updates within 2 minutes
  private static final String VERSION_OF_THIS_TOOL = "5.0.7";  // TODO: hardcoded for now, but eventually to sync with actual Solr version
  
  // private vars
  int delay;
  int multiDocBatchSize;
  boolean commit;
  int commitWithin;
  boolean optimize;
  String collection;
  String[] args;
  CloudSolrClient solrClient;
  XMLInputFactory xmlInputFactory;
  ExecutorService indexerPool;
  FileFilter fileFilter;
  
  static final String USAGE_STRING_SHORT =
      "Usage: java [SystemProperties] -jar cloudpost.jar [-h] [<file|folder> [<file|folder>...]]";

  /**
   * See usage() for valid command line usage
   * @param args the params on the command line
   */
  public static void main(String[] args) {
    info("CloudPostTool version " + VERSION_OF_THIS_TOOL);
    if (args.length == 0 || ("-help".equals(args[0]) || "--help".equals(args[0]) || "-h".equals(args[0]))) {
      usage();
    } else {
      final CloudPostTool t = parseArgsAndInit(args);
      t.execute();
    }
  }

  /**
   * After initialization, call execute to start the post job.
   */
  public void execute() {
    final long startTime = System.currentTimeMillis();

    info("Posting files to SolrCloud cluster zk " + this.solrClient.getZkHost());
	int numFilesPosted = postFiles(this.args);
	info(numFilesPosted + " files indexed.");
    
    if (commit) commit();
    if (optimize) optimize();
    displayTiming(System.currentTimeMillis() - startTime);
    
    try { solrClient.close(); } catch (IOException ignore) {}
  }
  
  /**
   * Pretty prints the number of milliseconds taken to post the content to Solr
   * @param millis the time in milliseconds
   */
  private void displayTiming(long millis) {
    SimpleDateFormat df = new SimpleDateFormat("H:mm:ss.SSS", Locale.getDefault());
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    System.out.println("Time spent: "+df.format(new Date(millis)));
  }

  /**
   * Parses incoming arguments and system params and initializes the tool
   * @param args the incoming cmd line args
   * @return an instance of CloudPostTool
   */
  protected static CloudPostTool parseArgsAndInit(String[] args) {
	  // Parse args
	  String collection = System.getProperty("c");
	  if (collection == null) {
        fatal("Specifying the collectionis mandatory.\n" + USAGE_STRING_SHORT);
      }
	
	  String zkHost = System.getProperty("zk", DEFAULT_ZK_HOST);
	  String routeField = System.getProperty("rf", DEFAULT_ROUTE_FIELD);
	  int delay = Integer.parseInt(System.getProperty("delay", String.valueOf(DEFAULT_DELAY)));
	  int numOfThreads = Integer.parseInt(System.getProperty("threads", String.valueOf(DEFAULT_NUMBER_OF_THREADS)));
	  int multiDocBatchSize = Integer.parseInt(System.getProperty("batchsize", String.valueOf(DEFAULT_MULTIDOC_BATCH_SIZE)));
	  int commitWithin = Integer.parseInt(System.getProperty("commitwithin", String.valueOf(DEFAULT_COMMIT_WITHIN_SECONDS)));
	  boolean commit = isOn(System.getProperty("commit",DEFAULT_COMMIT));
	  boolean optimize = isOn(System.getProperty("optimize",DEFAULT_OPTIMIZE));
	  
	  return new CloudPostTool(collection, zkHost, routeField, delay, numOfThreads, multiDocBatchSize, commit, commitWithin, optimize, args);
  }

  /**
   * Constructor which takes in all mandatory input for the tool to work.
   * Also see usage() for further explanation of the params.
   * @param collection : the Solr collection
   * @param zkHost : the ZooKeeper host(s)
   * @param routeField : the field to route documents on (use null to auto-lookup the uniqueKey from schema.xml)
   * @param delay : if recursive then delay will be the wait time between posts
   * @param numOfThreads : number of threads to use (only used for posting directory/multiple files)
   * @param multiDocBatchSize : size of the update-batches for multi-doc files
   * @param commit : if true, will commit at end of posting
   * @param commitWithin : the commit-within interval (in seconds) to use for each post
   * @param optimize : if true, will optimize at end of posting
   * @param args : a String[] of file/directory arguments
   */
  public CloudPostTool(String collection, String zkHost, String routeField, int delay, int numOfThreads, int multiDocBatchSize,
      boolean commit, int commitWithin, boolean optimize, String[] args) {
	   
	this.collection = collection;
    this.solrClient = new CloudSolrClient(zkHost);
	this.solrClient.setDefaultCollection(collection);
	this.solrClient.setIdField(routeField);
	
	this.indexerPool = Executors.newFixedThreadPool(numOfThreads);
	this.xmlInputFactory = XMLInputFactory.newInstance();
	this.fileFilter = getFileFilterFromFileTypes(DEFAULT_FILE_TYPES);
    this.delay = delay;
    this.multiDocBatchSize = multiDocBatchSize;
    this.commit = commit;
    this.commitWithin = commitWithin;
    this.optimize = optimize;
    this.args = args;
  }

  public CloudPostTool() {}
  

  private static void usage() {
    System.out.println
    (USAGE_STRING_SHORT+"\n\n" +
     "Supported System Properties and their defaults:\n"+
     "  -Dzk=<ZooKeeper host(s)> (default=" + DEFAULT_ZK_HOST + ")\n"+
     "  -Dc=<Target collection name>\n" +
     "  -Drf=<Field to route documents on> (default='" + DEFAULT_ROUTE_FIELD + "')\n"+
     "  -Dthreads=<Number of threads used when posting directory/multiple files> (default=" + DEFAULT_NUMBER_OF_THREADS + ")\n"+
     "  -Dbatchsize=<Maximum number of documents in a post batch> (default=" + DEFAULT_MULTIDOC_BATCH_SIZE + ")\n"+
     "  -Dcommit=yes|no (default=" + DEFAULT_COMMIT + ")\n"+
     "  -commitWithin=<Commit-within interval (in seconds) used for posting updates> (default=" + DEFAULT_COMMIT_WITHIN_SECONDS + ")\n"+
     "  -Doptimize=yes|no (default=" + DEFAULT_OPTIMIZE + ")\n"+
     "This is a simple command line tool for POSTing (zipped) xml files to a SolrCloud cluster.\n"+
     "NOTE: Specifying the collection is mandatory.");
  }

  /** Post all filenames provided in args
   * @param args array of file names
   * @return number of files posted
   * */
  public int postFiles(String [] args) {
    int filesPosted = 0;
    for (int j = 0; j < args.length; j++) {
      File srcFile = new File(args[j]);
      if(srcFile.isDirectory() && srcFile.canRead()) {
        filesPosted += postDirectory(srcFile);
      } else if (srcFile.isFile() && srcFile.canRead()) {
        filesPosted += postFiles(new File[] {srcFile});
      } else {
        File parent = srcFile.getParentFile();
        if(parent == null) parent = new File(".");
        String fileGlob = srcFile.getName();
        GlobFileFilter ff = new GlobFileFilter(fileGlob, false);
        File[] files = parent.listFiles(ff);
        if(files == null || files.length == 0) {
          warn("No files or directories matching "+srcFile);
          continue;          
        }
        filesPosted += postFiles(parent.listFiles(ff));
      }
    }
    
    indexerPool.shutdown();
    try {
    	indexerPool.awaitTermination(24, TimeUnit.HOURS); //give the index jobs 24 hours to finish
    } catch (InterruptedException e) {
		fatal("Execution of threads interrupted: " + e);
	}
    
    return filesPosted;
  }
  
  /** Post all filenames provided in args
   * @param files array of Files
   * @return number of files posted
   * */
  public int postFiles(File[] files) {
    int filesPosted = 0;
    Arrays.sort(files);
    for (File srcFile : files) {
      if(srcFile.isDirectory() && srcFile.canRead()) {
        filesPosted += postDirectory(srcFile);
      } else if (srcFile.isFile() && srcFile.canRead()) {
        filesPosted += postFilesInternal(new File[] {srcFile});
      } else {
        File parent = srcFile.getParentFile();
        if(parent == null) parent = new File(".");
        String fileGlob = srcFile.getName();
        GlobFileFilter ff = new GlobFileFilter(fileGlob, false);
        File[] fileList = parent.listFiles(ff);
        if(fileList == null || fileList.length == 0) {
          warn("No files or directories matching "+srcFile);
          continue;          
        }
        filesPosted += postFiles(fileList);
      }
    }
    return filesPosted;
  }
  
  /**
   * Posts a whole directory
   * @return number of files posted total
   */
  private int postDirectory(File dir) {
    if(dir.isHidden() && !dir.getName().equals("."))
      return(0);
    info("Indexing directory "+dir.getPath()+" ("+dir.listFiles(fileFilter).length+" files)");
    int posted = 0;
    posted += postFiles(dir.listFiles(fileFilter));
 
      for(File d : dir.listFiles()) {
        if(d.isDirectory()) {
          posted += postDirectory(d);
        }
      }
    return posted;
  }

  /**
   * Posts a list of file names
   * @return number of files posted
   */
  int postFilesInternal(File[] files) {
    int filesPosted = 0;
    for(File file : files) {
        if(!file.isFile() || file.isHidden()) continue;
        SolrUpdateRunner updater = new SolrUpdateRunner(this.solrClient, this.xmlInputFactory, file, delay, this.multiDocBatchSize, this.commitWithin);
        indexerPool.execute(updater);
        filesPosted++;
    }
    return filesPosted;
  }


  /**
   * Tests if a string is either "true", "on", "yes" or "1"
   * @param property the string to test
   * @return true if "on"
   */
  protected static boolean isOn(String property) {
    return("true,on,yes,1".indexOf(property) > -1);
  }
  
  static void warn(String msg) {
    System.err.println("CloudPostTool: WARNING: " + msg);
  }

  static void info(String msg) {
    System.out.println(msg);
  }

  static void fatal(String msg) {
    System.err.println("CloudPostTool: FATAL: " + msg);
    System.exit(2);
  }

  /**
   * Does a simple commit operation 
   */
  public void commit() {
    info("COMMITting Solr index changes...");
    try {
    	this.solrClient.commit();
    } catch (SolrServerException|IOException e) {
    	warn("Exception running COMMIT: " + e);
	}
  }

  /**
   * Does a simple optimize operation 
   */
  public void optimize() {
    info("Performing an OPTIMIZE...");
    try {
    	this.solrClient.optimize();
    } catch (SolrServerException|IOException e) {
    	warn("Exception running OPTIMIZE: " + e);
	}
  }

  public FileFilter getFileFilterFromFileTypes(String fileTypes) {
    String glob;
    if(fileTypes.equals("*"))
      glob = ".*";
    else
      glob = "^.*\\.(" + fileTypes.replace(",", "|") + ")$";
    return new GlobFileFilter(glob, true);
  }


  /**
   * Inner class to filter files based on glob wildcards
   */
  class GlobFileFilter implements FileFilter
  {
    private String _pattern;
    private Pattern p;
    
    public GlobFileFilter(String pattern, boolean isRegex)
    {
      _pattern = pattern;
      if(!isRegex) {
        _pattern = _pattern
            .replace("^", "\\^")
            .replace("$", "\\$")
            .replace(".", "\\.")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("+", "\\+")
            .replace("*", ".*")
            .replace("?", ".");
        _pattern = "^" + _pattern + "$";
      }
      
      try {
        p = Pattern.compile(_pattern,Pattern.CASE_INSENSITIVE);
      } catch(PatternSyntaxException e) {
        fatal("Invalid type list "+pattern+". "+e.getDescription());
      }
    }
    
    @Override
    public boolean accept(File file)
    {
      return p.matcher(file.getName()).find();
    }
  }
}
