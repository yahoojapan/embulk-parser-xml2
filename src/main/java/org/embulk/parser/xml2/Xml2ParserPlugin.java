package org.embulk.parser.xml2;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

public class Xml2ParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task
    {
        // configuration option 1 (required integer)
        @Config("option1")
        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
        @Config("option2")
        @ConfigDefault("\"myvalue\"")
        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        @Config("option3")
        @ConfigDefault("null")
        public Optional<String> getOption3();

        // if you get schema from config or data source
        @Config("columns")
        public SchemaConfig getColumns();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Write your code here :)
        throw new UnsupportedOperationException("Xml2ParserPlugin.run method is not implemented yet");
    }
}
