package org.apache.hadoop.xml.input;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Reads element in records that are delimited by a specific Start and end tag.
 */
public class XmlStaxInputFormat extends FileInputFormat<Text, Text> {

    public static final String START_TAG_KEY = "xml.stax.start.tag";

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public XmlStaxFileRecordReader createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new XmlStaxFileRecordReader();
    }

}
