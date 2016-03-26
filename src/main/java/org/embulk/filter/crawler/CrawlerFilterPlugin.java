package org.embulk.filter.crawler;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlerFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("max_depth_of_crawling")
        @ConfigDefault("null")
        public Optional<Integer> getMaxDepthOfCrawling();

        @Config("number_of_crawlers")
        @ConfigDefault("1")
        public int getNumberOfCrawlers();

        @Config("max_pages_to_fetch")
        @ConfigDefault("-1")
        public int getMaxPagesToFetch();

        @Config("target_key")
        public String getTargetKey();

        @Config("crawl_storage_folder")
        public String getCrawlStorageFolder();

        @Config("politeness_delay")
        @ConfigDefault("null")
        public Optional<Integer> getPolitenessDelay();

        @Config("user_agent_string")
        @ConfigDefault("null")
        public Optional<String> getUserAgentString();

        @Config("keep_input")
        @ConfigDefault("true")
        public boolean getKeepInput();

        @Config("output_prefix")
        @ConfigDefault("\"\"")
        public String getOutputPrefix();

        @Config("should_not_visit_pattern")
        @ConfigDefault("null")
        public Optional<String> getShouldNotVisitPattern();

        @Config("connection_timeout")
        @ConfigDefault("null")
        public Optional<Integer> getConnectionTimeout();

        @Config("socket_timeout")
        @ConfigDefault("null")
        public Optional<Integer> getSocketTimeout();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0;
        builder.addAll(getOutputColumns(i, task.getOutputPrefix()));

        Schema outputSchema = new Schema(builder.build());
        control.run(task.dump(), outputSchema);
    }

    /**
     * @param i
     * @param outputPrefix
     * @return
     */
    private List<Column> getOutputColumns(int i, String outputPrefix) {
        List<Column> list = Lists.newArrayList();
        list.add(new Column(i++, outputPrefix + Constants.URL, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.DOMAIN, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.SUBDOMAIN, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.PATH, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.ANCHOR, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.PARENT_URL, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.CONTENT_CHARSET, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.REDIRECT_TO_URL, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.LANGUAGE, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.STATUS_CODE, Types.LONG));
        list.add(new Column(i++, outputPrefix + Constants.TITLE, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.TEXT, Types.STRING));
        list.add(new Column(i++, outputPrefix + Constants.HTML, Types.STRING));
        return list;
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Column keyNameColumn = inputSchema.lookupColumn(task.getTargetKey());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private CrawlController controller = getController();

            @Override
            public void finish()
            {
                for (Object object : controller.getCrawlersLocalData()) {
                    CrawlStat crawlStat = (CrawlStat) object;
                    for (Map<String, Object> map : crawlStat.getPages()) {
                        for (Column outputColumn : outputSchema.getColumns()) {
                            final Object value = map.get(outputColumn.getName());
                            setValue(value, outputColumn);
                        }
                        builder.addRecord();
                    }
                }
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            @Override
            public void add(Page page)
            {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    controller.addSeed(reader.getString(keyNameColumn));
                }
                Map<String, Object> customData = Maps.newHashMap();
                customData.put("output_prefix", task.getOutputPrefix());
                if (task.getShouldNotVisitPattern().isPresent()) {
                    customData.put("should_not_visit_pattern", task.getShouldNotVisitPattern().get());
                }
                controller.setCustomData(customData);
                controller.start(EmbulkCrawler.class, task.getNumberOfCrawlers());
            }

            /**
             * @param seeds
             */
            private void setValue(Object value, Column column)
            {
                if (value == null) {
                    builder.setNull(column);
                }
                else if (column.getType().equals(Types.STRING)) {
                    builder.setString(column, (String) value);
                }
                else if (column.getType().equals(Types.LONG)) {
                    builder.setLong(column, (Integer) value);
                }
            }

            /**
             * @param seeds
             * @return
             */
            private CrawlController getController()
            {
                CrawlConfig config = new CrawlConfig();
                String directoryPath = String.format(task.getCrawlStorageFolder(), UUID.randomUUID());
                File dir = new File(directoryPath);
                dir.mkdirs();

                config.setCrawlStorageFolder(directoryPath);
                if (task.getMaxDepthOfCrawling().isPresent()) {
                    config.setMaxDepthOfCrawling(task.getMaxDepthOfCrawling().get());
                }

                config.setMaxPagesToFetch(task.getMaxPagesToFetch());
                if (task.getPolitenessDelay().isPresent()) {
                    config.setPolitenessDelay(task.getPolitenessDelay().get());
                }
                if (task.getUserAgentString().isPresent()) {
                    config.setUserAgentString(task.getUserAgentString().get());
                }
                if (task.getSocketTimeout().isPresent()) {
                    config.setSocketTimeout(task.getSocketTimeout().get());
                }
                if (task.getConnectionTimeout().isPresent()) {
                    config.setConnectionTimeout(task.getConnectionTimeout().get());
                }

                PageFetcher pageFetcher = new PageFetcher(config);
                RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
                RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
                CrawlController crawlController = null;
                try {
                    crawlController = new CrawlController(config, pageFetcher, robotstxtServer);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return crawlController;
            }
        };
    }
}
