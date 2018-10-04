package org.apache.hadoop.xml.input;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class XmlStaxFileRecordReader extends RecordReader<Text, Text> {

    private static final Log LOG = LogFactory.getLog(XmlStaxFileRecordReader.class.getName());
    private String startTag;
    private FSDataInputStream fsdatainstream;
    private InputStream concatInputStream;
    private BufferedInputStream bif;
    private Text key = new Text();
    private Text value = new Text();
    private String fileOutput;
    private XMLStreamReader reader;
    private boolean processed = false;
    private int nodeCount = 0;

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) is;
        startTag = tac.getConfiguration().get(XmlStaxInputFormat.START_TAG_KEY);

        Path file = fileSplit.getPath();
        fileOutput = file.getName();

        FileSystem fs = file.getFileSystem(tac.getConfiguration());
        fsdatainstream = fs.open(fileSplit.getPath());
        bif = new BufferedInputStream(fsdatainstream);
        XMLInputFactory factory = XMLInputFactory.newInstance();
        // Wrap up XML with temporary element for XML that may not be full structured and concat it
        factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
        List<InputStream> streams = Arrays.asList(new ByteArrayInputStream("<wrapxmlrootup>".getBytes()), bif,
                new ByteArrayInputStream("</wrapxmlrootup>".getBytes()));
        concatInputStream = new SequenceInputStream(Collections.enumeration(streams));

        try {
            reader = factory.createXMLStreamReader(concatInputStream);
        } catch (XMLStreamException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // This breaks record up by search for the startTag Element and passes it to mapper
        try {
            while (reader.hasNext()) {
                reader.next();
                switch (reader.getEventType()) {
                    case XMLEvent.START_ELEMENT: {
                        //Grab the XML Element that starts with a specific tag and turn it into a string object and send it to mapper
                        if (reader.getLocalName().equalsIgnoreCase(startTag)) {
                            LOG.trace("StartXMLKey: " + reader.getLocalName());
                            Transformer transformer = TransformerFactory.newInstance().newTransformer();
                            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                            StringWriter stringWriter = new StringWriter();
                            transformer.transform(new StAXSource(reader), new StreamResult(stringWriter));
                            value.set(stringWriter.toString());
                            LOG.trace("XML Object: " + stringWriter.toString());
                            key.set(fileOutput);
                            nodeCount++;
                            return true;
                        }
                    }
                }

            }
        } catch (IllegalArgumentException | XMLStreamException | TransformerFactoryConfigurationError
                | TransformerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        LOG.info("XML " + startTag + " NodeCount: " + nodeCount);
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (XMLStreamException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (concatInputStream != null) {
            concatInputStream.close();
        }
        if (bif != null) {
            bif.close();
        }
        if (fsdatainstream != null) {
            fsdatainstream.close();
        }
    }


}
