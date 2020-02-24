Embulk::JavaPlugin.register_output(
  "sf_bulk_api", "org.embulk.output.sf_bulk_api.SfBulkApiFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
