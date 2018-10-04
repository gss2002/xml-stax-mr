package org.apache.hadoop.xml;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;


/**
 *
 */
public class XmlFormatMapper extends AutoProgressMapper<Text, Text, NullWritable, Text> {
    private static final Log LOG = LogFactory.getLog(XmlFormatMapper.class.getName());

    private static Map<String, String> elementMap;
    private static Map<String, String> attrMap;
    private Configuration conf;
    private XMLOutputter output;
    private MultipleOutputs<NullWritable, Text> out;

    private String convFunc;
    private String convValue;
    private String convName;

    public XmlFormatMapper() {
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        out = new MultipleOutputs<NullWritable, Text>(context);

        this.conf = context.getConfiguration();
        if (this.conf.get("xml.convert.elements") != null) {
            elementStringToMap(this.conf.get("xml.convert.elements"));
        }
        if (this.conf.get("xml.convert.attrs") != null) {
            attrStringToMap(this.conf.get("xml.convert.attrs"));
        }

    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String outputPath = key.toString();
        String xmlString = value.toString();
        LOG.trace("XMLStringIn From HDFS: " + xmlString);

        SAXBuilder builder = new SAXBuilder();
        Reader in = new StringReader(xmlString);
        try {

            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            iterateXmlNodes(root.getChildren());
            this.output = new XMLOutputter(Format.getPrettyFormat().setOmitDeclaration(true));
            LOG.trace("XMLStringOut to HDFS: " + this.output.outputString(doc));

            out.write(NullWritable.get(), new Text(this.output.outputString(doc)), outputPath);

        } catch (JDOMException je) {
            je.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    public void iterateXmlNodes(List<Element> xmlElementList) {

        for (Element element : xmlElementList) {
            if (!(element.getChildren().isEmpty())) {
                if (element.hasAttributes()) {
                    iterateAttributes(element.getAttributes());
                }
                iterateXmlNodes(element.getChildren());
            } else {
                if (element.hasAttributes()) {
                    iterateAttributes(element.getAttributes());
                }
                if (elementMap != null) {
                    if (!(elementMap.isEmpty())) {
                        if (elementMap.containsKey(element.getParentElement().getName() + "." + element.getName())) {
                            LOG.debug("ParentElement.ElementName: " + element.getParentElement().getName() + "."
                                    + element.getName());
                            this.convFunc = elementMap
                                    .get(element.getParentElement().getName() + "." + element.getName());
                            this.convValue = element.getText();
                            if (this.convValue != null) {
                                if (!(this.convValue.equalsIgnoreCase(""))) {
                                    if (convFunc.equalsIgnoreCase("convert_f")) {
                                        element.setText(convertFarenheit(convName, convValue));
                                    }
                                }
                            }

                        }
                    }
                }
                LOG.debug("Parent Element: " + element.getParentElement().getName() + ":: Element: " + element.getName()
                        + "=" + element.getText());

            }
        }
    }

    public void iterateAttributes(List<Attribute> attrList) {
        for (Attribute attr : attrList) {
            if (attrMap != null) {

                if (!(attrMap.isEmpty())) {
                    if (elementMap.containsKey(attr.getParent().getName() + "." + attr.getName())) {
                        this.convFunc = elementMap.get(attr.getParent().getName() + "." + attr.getName());
                        this.convValue = attr.getValue();

                        if (this.convValue != null) {
                            if (!(this.convValue.equalsIgnoreCase(""))) {
                                if (convFunc.equalsIgnoreCase("convert_f")) {
                                    attr.setValue(convertFarenheit(convName, convValue));
                                }
                            }
                        }
                    }
                }
                LOG.debug("Attr Parent Element: " + attr.getParent().getName() + " :: Attr: " + attr.getName() + "="
                        + attr.getValue());

            }
        }
    }

    public void elementStringToMap(String elementString) {
        elementMap = new HashMap<String, String>();
        if (elementString.contains(",")) {
            String[] elementArray = elementString.split(",");
            for (String element : elementArray) {
                LOG.debug("ElementMap: " + element.split("=")[0] + "::" + element.split("=")[1]);
                elementMap.put(element.split("=")[0], element.split("=")[1]);
            }
        } else {
            LOG.debug("ElementMap: " + elementString.split("=")[0] + "::" + elementString.split("=")[1]);

            elementMap.put(elementString.split("=")[0], elementString.split("=")[1]);
        }
    }

    public void attrStringToMap(String attrString) {
        attrMap = new HashMap<String, String>();
        if (attrString.contains(",")) {
            String[] attrArray = attrString.split(",");
            for (String attr : attrArray) {
                LOG.debug("AttrMap: " + attr.split("=")[0] + "::" + attr.split("=")[1]);
                attrMap.put(attr.split("=")[0], attr.split("=")[1]);
            }
        } else {
            LOG.debug("AttrMap: " + attrString.split("=")[0] + "::" + attrString.split("=")[1]);
            attrMap.put(attrString.split("=")[0], attrString.split("=")[1]);
        }
    }

    public String convertFarenheit(String attrOrElementName, String convValueIn) {
        double celsius = Double.parseDouble(convValueIn);
        double fahrenheit = (9.0/5.0)*celsius + 32;
        return Double.toString(fahrenheit);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }

    public String skipInValidXMLChars(String in) {
        StringBuffer out = new StringBuffer();
        char current;

        if (in == null || ("".equals(in)))
            return "";
        for (int i = 0; i < in.length(); i++) {
            current = in.charAt(i);
            if ((current == 0x9) || (current == 0xA) || (current == 0xD) || ((current >= 0x20) && (current <= 0xD7FF))
                    || ((current >= 0xE000) && (current <= 0xFFFD)) || ((current >= 0x10000) && (current <= 0x10FFFF)))
                out.append(current);
        }
        return out.toString();
    }
}
