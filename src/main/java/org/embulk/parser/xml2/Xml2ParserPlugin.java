package org.embulk.parser.xml2;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Stack;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.embulk.config.Config;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.Timestamps;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.slf4j.Logger;

public class Xml2ParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("root")
        public String getRoot();

        @Config("schema")
        public SchemaConfig getSchema();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getSchema().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(final TaskSource taskSource, final Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchema());
        
        final String rootElementName = task.getRoot();
        
        SAXParser parser = createXMLParser();
        try (FileInputInputStream is = new FileInputInputStream(input)) {
            final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);
            while(is.nextFile()) {
                parser.parse(is, new DefaultHandler() {
                    
                    private Stack<String> currentXPath;
                    private boolean isElementMatch = false;
                    private StringBuffer valueBuf = null;
                    private String currentSubElementName = null;
                    private int extractedTotalPageNum = 0;
                    private Logger logger = Exec.getLogger(getClass());;
                    
                    @Override
                    public void startDocument() throws SAXException {
                        currentXPath = new Stack<String>();
                        logger.debug("start parsing document.");
                    }

                    @Override
                    public void startElement(String uri, String localName, String qName, Attributes attributes)
                            throws SAXException {
                        currentXPath.push(qName);
                        
                        String path = toXPath(currentXPath);
                        // if this element is not under the root element, skip process.
                        if (!isChildOfRootElement(rootElementName, path)) {
                            return;
                        }
                        
                        String subElementName = getSubElementName(rootElementName, path);
                        if (subElementName == null) {
                            return;
                        }
                        currentSubElementName = subElementName;
                        
                        if(getColumn(schema, subElementName) != null){
                            isElementMatch = true;
                            valueBuf = new StringBuffer();
                        }
                    }

                    private Column getColumn(Schema schema, String subElementName) {
                        for (Column c : schema.getColumns()){
                            if (c.getName().equals(subElementName)) return c;
                        }
                        return null;
                    }

                    private String getSubElementName(String rootElementName, String path) {
                        if (rootElementName.equals(path) || !path.startsWith(rootElementName)) {
                            return null;
                        } else {
                            return path.substring(rootElementName.length() + 1);
                        }
                    }

                    private boolean isChildOfRootElement(String rootElementName, String xPath) {
                        return xPath.startsWith(rootElementName);
                    }

                    private String toXPath(Stack<String> stack) {
                        StringBuffer buf = new StringBuffer();
                        for(String p : stack) {
                            if (buf.length() > 0) {
                                buf.append("/");
                            }
                            buf.append(p);
                        }
                        return buf.toString();
                    }

                    @Override
                    public void endElement(String uri, String localName, String qName) throws SAXException {
                        
                        String path = toXPath(currentXPath);
                        if (path.equals(rootElementName)) {
                            pageBuilder.addRecord();
                            extractedTotalPageNum++;
                        }
                        
                        // if isElementMatch is true, set data to Page.
                        if (isElementMatch) {
                            final String strValue = valueBuf.toString();
                            
                            schema.visitColumns(new ColumnVisitor() {

                                @Override
                                public void timestampColumn(Column column) {
                                    if (!column.getName().equals(currentSubElementName)) return;
                                    TimestampParser tsparser = timestampParsers[column.getIndex()];
                                    Timestamp time = tsparser.parse(strValue);
                                    pageBuilder.setTimestamp(column, time);
                                }

                                @Override
                                public void stringColumn(Column column) {
                                    if (!column.getName().equals(currentSubElementName)) return;
                                    pageBuilder.setString(column, strValue);
                                }

                                @Override
                                public void longColumn(Column column) {
                                    if (!column.getName().equals(currentSubElementName)) return;
                                    pageBuilder.setLong(column, Long.parseLong(strValue));
                                }

                                @Override
                                public void doubleColumn(Column column) {
                                    if (!column.getName().equals(currentSubElementName)) return;
                                    pageBuilder.setDouble(column, Double.parseDouble(strValue));
                                }

                                @Override
                                public void booleanColumn(Column column) {
                                    if (!column.getName().equals(currentSubElementName)) return;
                                    pageBuilder.setBoolean(column, Boolean.parseBoolean(strValue));
                                }

                                @Override
                                public void jsonColumn(Column column) {
                                    if (!column.getName().equals(currentSubElementName)) return;
                                    // treat json as string.
                                    pageBuilder.setString(column, strValue);
                                }
                            });
                        }
                        
                        currentXPath.pop();
                        isElementMatch = false;
                        valueBuf = null;
                        currentSubElementName = null;
                    } 
                    
                    @Override
                    public void characters(char[] ch, int offset, int length) {
                        if (!isElementMatch) {
                            return;
                        }
                        valueBuf.append(ch,offset,length);
                    }
                    
                    @Override
                    public void endDocument() {
                        logger.debug("end parsing document. total extracted page count is : " + extractedTotalPageNum);
                    }
                });
                pageBuilder.flush();
            }
            pageBuilder.finish();
            pageBuilder.close();
        } catch (SAXException | IOException e) {
            Throwables.propagate(e); // TODO error handling
        }
    }

    private SAXParser createXMLParser() {
        SAXParser parser;
        try {
            parser = SAXParserFactory.newInstance().newSAXParser();
            return parser;
        } catch (ParserConfigurationException | SAXException e) {
            Throwables.propagate(e); // TODO error handling
        }
        return null;
    }
}
