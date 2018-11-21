package org.apache.hadoop.xml;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.xml.input.XmlStaxInputFormat;
import org.apache.hadoop.xml.output.XmlFileOutputFormat;


public class XmlMrDriver {

    /**
     * For processing XML file using Hadoop MapReduce
     *
     * @param args
     */
    static Options options;
    static String xmlInputPath;
    static String xmlOutputPath;
    static String xmlStartTag;
    static String xmlElements;
    static String xmlAttributes;

    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration();
            String[] otherArgs = null;
            try {
                otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            } catch (IOException e4) {
                // TODO Auto-generated catch block
                e4.printStackTrace();
            }
            options = new Options();
            options.addOption("xml_input_path", true, "Input Folder/Path in HDFS of Xml files to be converted");
            options.addOption("xml_output_path", true, "Output Folder/Path in HDFS of Xml files to be converted");
            options.addOption("xml_start_tag", true,
                    "Start Tag for the RecordReader - weather, METAR");
            options.addOption("xml_elements", true,
                    "Xml Elements to convert in bulk - METAR.temp_c=convert_f;METAR.dewpoint_c=convert_f");
            options.addOption("xml_attributes", true, "XML Attributes to convert in bulk - METAR.temp_c=convert_f;METAR.dewpoint_c=convert_f");
            options.addOption("help", false, "Display help");
            CommandLineParser parser = new XmlMrParser();
            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, otherArgs);
            } catch (ParseException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }

            if (cmd.hasOption("xml_input_path") && cmd.hasOption("xml_output_path") && cmd.hasOption("xml_start_tag")) {
                xmlInputPath = cmd.getOptionValue("xml_input_path");
                xmlOutputPath = cmd.getOptionValue("xml_output_path");
                xmlStartTag = cmd.getOptionValue("xml_start_tag");

            } else {
                System.out.println("xml_input_path or xml_output_path or xml_start_tag");
                missingParams();
                System.exit(0);
            }

            if (cmd.hasOption("xml_elements") || cmd.hasOption("xml_attributes")) {
                if (cmd.hasOption("xml_elements")) {
                    xmlElements = cmd.getOptionValue("xml_elements");
                } else {
                    xmlElements = "";
                }
                if (cmd.hasOption("xml_attributes")) {
                    xmlAttributes = cmd.getOptionValue("xml_attributes");
                } else {
                    xmlAttributes = "";
                }
            } else {
                System.out.println("xml_elements or xml_attributes are missing");
                missingParams();
                System.exit(0);
            }
            String jobName = xmlInputPath.replace("/", "_");

            if (System.getProperty("oozie.action.conf.xml") != null) {
                conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));

            }
            // propagate delegation related props from launcher job to MR job
            if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
                System.out.println(
                        "HADOOP_TOKEN_FILE_LOCATION is NOT NULL: " + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
                conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            }

            //Set xml.stax.start.tag to pass the start tag of elements to XmlStaxFileRecordReader/InputFormat
            conf.set("xml.stax.start.tag", xmlStartTag);
            conf.set("xml.convert.elements", xmlElements);
            conf.set("xml.convert.attrs", xmlAttributes);

            String input = xmlInputPath;
            String output = xmlOutputPath;
            Path outPath = new Path(output);

            conf.set("io.serializations",
                    "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

            Job job = Job.getInstance(conf, "XmlMr-" + jobName);

            FileInputFormat.setInputPaths(job, input);
            job.addCacheFile(new Path("/apps/xmlmr/jdom.jar").toUri());
            job.addArchiveToClassPath(new Path("/apps/xmlmr/jdom.jar"));
            job.setJarByClass(XmlMrDriver.class);
            job.setMapperClass(XmlFormatMapper.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(XmlStaxInputFormat.class);
            LazyOutputFormat.setOutputFormatClass(job, XmlFileOutputFormat.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, outPath);
            FileSystem dfs = FileSystem.get(outPath.toUri(), conf);

            if (dfs.exists(outPath)) { dfs.delete(outPath, true); }

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void missingParams() {
        String header = "XmlMrDriver";
        String footer = "\nPlease report issues";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("get", header, options, footer, true);
        System.exit(0);
    }

}

