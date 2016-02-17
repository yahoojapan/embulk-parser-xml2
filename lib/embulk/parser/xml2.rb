Embulk::JavaPlugin.register_parser(
  "xml2", "org.embulk.parser.xml2.Xml2ParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
