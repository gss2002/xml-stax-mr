package org.apache.hadoop.xml.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

public class XmlFileOutputFormat extends TextOutputFormat<Text, NullWritable> {

    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outDir}, job.getConfiguration());
    }

    /**
     * Get the default path and filename for the output format.
     *
     * @param context   the task context
     * @param extension an extension to add to the filename
     * @return a full path $output/_temporary/$taskid/part-[mr]-$id
     * @throws IOException
     */
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getOutputName(context));
    }
}