/**
 * The MIT License (MIT)
 *
 * Copyright (C) 2016 Yahoo Japan Corporation. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.embulk.parser.xml2;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.parser.xml2.Xml2ParserPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.InputStreamFileInput;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestXml2ParserPlugin {
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private Xml2ParserPlugin plugin;

    private static String PATH_PREFIX;

    @BeforeClass
    public static void initializeConstant() {
        PATH_PREFIX = Xml2ParserPlugin.class.getClassLoader().getResource("sample_01.xml").getPath();
    }

    @Before
    public void createResource() {
        plugin = new Xml2ParserPlugin();
    }

    @Test
    public void testTransaction() {
        ConfigSource config = config();
        plugin.transaction(config, new ParserPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema) {
            }
        });
    }

    @Test
    public void testFile() throws FileNotFoundException {
        
        ConfigSource config = config();
        final Schema schema = config.loadConfig(Xml2ParserPlugin.PluginTask.class).getSchema().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, new ParserPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema schema) {
            }
        });
        
        // the list contains result.
        final List<Map<String,Object>> resultList = new LinkedList<Map<String,Object>>();
        
        plugin.run(task.dump(), schema,
                new InputStreamFileInput(Exec.getBufferAllocator(), new FileInputStream(new File(PATH_PREFIX))),
                new TransactionalPageOutput() {

                    private final PageReader reader = new PageReader(schema);

                    @Override
                    public void add(Page page) {
                        reader.setPage(page);
                        
                        while (reader.nextRecord()) {
                            final Map<String, Object> record = new HashMap<String, Object>();
                            for (Column column : schema.getColumns()) {
                                column.visit(new ColumnVisitor() {
                                    @Override
                                    public void timestampColumn(Column column) {
                                        if (reader.isNull(column)) {
                                            record.put(column.getName(), null);
                                            return;
                                        }
                                        record.put(column.getName(), reader.getTimestamp(column));
                                    }

                                    @Override
                                    public void stringColumn(Column column) {
                                        if (reader.isNull(column)) {
                                            record.put(column.getName(), null);
                                            return;
                                        }
                                        record.put(column.getName(), reader.getString(column));
                                    }

                                    @Override
                                    public void longColumn(Column column) {
                                        if (reader.isNull(column)) {
                                            record.put(column.getName(), null);
                                            return;
                                        }
                                        record.put(column.getName(), reader.getLong(column));
                                    }

                                    @Override
                                    public void doubleColumn(Column column) {
                                        if (reader.isNull(column)) {
                                            record.put(column.getName(), null);
                                            return;
                                        }
                                        record.put(column.getName(), reader.getDouble(column));
                                    }

                                    @Override
                                    public void booleanColumn(Column column) {
                                        if (reader.isNull(column)) {
                                            record.put(column.getName(), null);
                                            return;
                                        }
                                        record.put(column.getName(), reader.getBoolean(column));
                                    }

                                    @Override
                                    public void jsonColumn(Column column) {
                                        if (reader.isNull(column)) {
                                            record.put(column.getName(), null);
                                            return;
                                        }
                                        record.put(column.getName(), reader.getString(column));
                                    }
                                });
                            }
                            resultList.add(record);
                        }
                    }

                    @Override
                    public void finish() {
                    }

                    @Override
                    public void close() {
                    }

                    @Override
                    public void abort() {
                    }

                    @Override
                    public TaskReport commit() {
                        return Exec.newTaskReport();
                    }
                });
        
        //assert...
        for (Map<String,Object> r : resultList) {
            System.out.println(r);
        }
        
        assertEquals(2,resultList.size());
        
        Map<String, Object> record0 = resultList.get(0);
        assertEquals("Wikipedia:アップロードログ 2004年4月",record0.get("title"));
        assertEquals(1L,record0.get("id"));
        assertEquals("なんか書く",record0.get("revision/text"));
        assertEquals(1083336360L * 1000L, ((Timestamp)record0.get("revision/timestamp")).toEpochMilli());
        
        Map<String, Object> record1 = resultList.get(1);
        assertEquals("アンパサンド",record1.get("title"));
        assertEquals(5L,record1.get("id"));
        assertEquals("アンパサンドとは\n「…と…」を意味する記号である。",record1.get("revision/text"));
        assertEquals(1449883580L * 1000L, ((Timestamp)record1.get("revision/timestamp")).toEpochMilli());   
    }

    private ConfigSource config() {
        return Exec.newConfigSource().set("in", inputConfig()).set("root", "mediawiki/page")
                .set("schema", schemaConfig()).set("out", outputConfig());
    }

    private ImmutableMap<String, Object> inputConfig() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> outputConfig() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "stdout");
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig() {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "title", "type", "string"));
        builder.add(ImmutableMap.of("name", "revision/timestamp", "type", "timestamp", "format", "%Y-%m-%dT%H:%M:%SZ", "timezone", "UTC"));
        builder.add(ImmutableMap.of("name", "revision/text", "type", "string"));
        return builder.build();
    }
}
