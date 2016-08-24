package com.dionoid.solr;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SolrUpdateRunner implements Runnable{

    public static final String ADD = "add";
    public static final String DELETE = "delete";
    public static final String DOC = "doc";
   
    private final CloudSolrClient solrClient;
    private final XMLInputFactory inputFactory;
    private final File file;
    private final int delay;
    private final int multiDocBatchSize;
    private final int commitWithin;
    
    public SolrUpdateRunner(CloudSolrClient solrClient, XMLInputFactory inputFactory, File file, int delay, int multiDocBatchSize, int commitWithin) {
        this.solrClient = solrClient;
        this.inputFactory = inputFactory;
        this.file = file;
        this.delay = delay;
        this.multiDocBatchSize = multiDocBatchSize;
        this.commitWithin = commitWithin;
    }
    
    @Override
    public void run() {
        try {
        	System.out.println("READing file " + this.file.getName());
        	int totalUpdates = postBatches();
        	System.out.println("Done POSTing all " + totalUpdates + " updates from " + this.file.getName());
            if (this.delay > 0) Thread.sleep(this.delay * 1000);
        } catch (Exception e) {
        	if (e instanceof InterruptedException) Thread.currentThread().interrupt();
			String error = "[error posting file " + this.file.getName() + "] : " + e;
			System.err.println(error);
        }
    }

    protected int postBatches() throws XMLStreamException, IOException {
    	XMLStreamReader parser = null;
		ZipInputStream zipStream = null;
		String filename = this.file.getName();
		
		try {
			//if zip, get filestream from it
			if (this.file.getAbsolutePath().toLowerCase().endsWith(".zip") || this.file.getAbsolutePath().toLowerCase().endsWith(".gz")) {
		        zipStream = new ZipInputStream(new FileInputStream(this.file));
		        ZipEntry entry = zipStream.getNextEntry(); //only handle a single .xml file insize the .zip
		        if (entry != null && entry.getName().toLowerCase().endsWith(".xml")) {
		        	System.out.println(">> Found xml file " + entry.getName() + " inside " + filename);
		        	parser = inputFactory.createXMLStreamReader(zipStream);
		        } else {
		        	throw new IOException("Cannot read .xml file from " + filename);
		        }
			} else { //just xml
				parser = inputFactory.createXMLStreamReader(new FileInputStream(this.file));
			}
	    	
	    	List<SolrInputDocument> docs = null;
	    	List<String> deleteIds = null;
	    	int docsInBatch = 0;
	    	int totalDocCount = 0;

    		while (true) {
    			if (docsInBatch >= this.multiDocBatchSize) {
					totalDocCount += sendBatch(docs, deleteIds, 10, 3);
					if (docs != null) docs.clear();
					if (deleteIds != null) deleteIds.clear();
		            docsInBatch = 0;
				}
    			int event = parser.next();
    			switch (event) {
	    			case XMLStreamConstants.END_DOCUMENT:
	    				parser.close();
	    				totalDocCount += sendBatch(docs, deleteIds, 10, 3);
	    				return totalDocCount;
	    			case XMLStreamConstants.START_ELEMENT:
	    				String currTag = parser.getLocalName();
	    				if (ADD.equals(currTag)) {
	    					if (docs == null) docs = new ArrayList<>();
	    				} else if (DOC.equals(currTag)) {
	    					SolrInputDocument doc = SolrXmlLoader.readDoc(parser);

	    					//HACK: set 'assetProduct' field to 'medline'
	    					doc.setField("assetProduct", "medline");
	    							
	    					docs.add(doc);
	    					docsInBatch++;
	    				} else if (DELETE.equals(currTag)) {
	    					if (deleteIds == null) deleteIds = new ArrayList<String>();
	    					List<String> deleteIdBatch = SolrXmlLoader.getDeleteIds(parser);
	    					docsInBatch += deleteIdBatch.size();
	    					deleteIds.addAll(deleteIdBatch);
		    			} else {
							//other elements (COMMIT, OPTIMIZE, etc.) are not supported
							throw new IOException("Found unsupported element '" + currTag + "' in " + filename);
						}	
	    				break;
    			}
    		}
    	} finally {
    		if (parser != null) parser.close();
    		if (zipStream != null) zipStream.close();
    	}
    }

    protected int sendBatch(List<SolrInputDocument> docs, List<String> deleteIds, int waitBeforeRetry, int maxRetries) {
    	UpdateRequest updateRequest = new UpdateRequest();
    	updateRequest.setCommitWithin(this.commitWithin * 1000); // best practice for "SolrJ and HTTP and client indexing" as in https://lucidworks.com/blog/2013/08/23/understanding-transaction-logs-softcommit-and-commit-in-sorlcloud/
    	String filename = this.file.getName();
    	try { 
        	int numOfDocs = 0;
			if (docs != null) numOfDocs += docs.size();
			if (deleteIds != null) numOfDocs += deleteIds.size();
			if (numOfDocs == 0) return 0;
            if (docs != null) updateRequest.add(docs);
            if (deleteIds != null) updateRequest.deleteById(deleteIds);
            solrClient.request(updateRequest);
            System.out.println("Succesfully POSTed a batch with " + numOfDocs + " updates from " + filename);
            return numOfDocs;
        } catch (Exception exc) {
            Throwable rootCause = SolrException.getRootCause(exc);
            boolean wasCommError =
                    (rootCause instanceof ConnectException ||
                            rootCause instanceof ConnectTimeoutException ||
                            rootCause instanceof NoHttpResponseException ||
                            rootCause instanceof SocketException);

            if (wasCommError) {
                if (--maxRetries > 0) {
                	System.err.println("ERROR: " + rootCause + " ... Sleeping for "
                            + waitBeforeRetry + " seconds before re-try ...");
                    try {
                        Thread.sleep(waitBeforeRetry * 1000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return sendBatch(docs, deleteIds, waitBeforeRetry, maxRetries);
                } else {
                    throw new RuntimeException("No more retries available!", exc);
                }
            } else {
                throw new RuntimeException(exc);
            }
        } finally {
        	updateRequest.clear();
        	updateRequest = null;
        }
    }
}
