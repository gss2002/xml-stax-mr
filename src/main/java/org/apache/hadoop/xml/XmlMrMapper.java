package org.apache.hadoop.xml;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Base mapper class that is convenient place for common functionality.
 * Other specific mappers are highly encouraged to inherit from this class.
 */
public abstract class XmlMrMapper<KI, VI, KO, VO> extends Mapper<KI, VI, KO, VO> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        // TODO Auto-generated method stub

    }
}