Embulk::JavaPlugin.register_filter(
  "crawler", "org.embulk.filter.crawler.CrawlerFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
