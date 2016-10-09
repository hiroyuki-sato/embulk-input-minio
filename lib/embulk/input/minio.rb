Embulk::JavaPlugin.register_input(
  "minio", "org.embulk.input.minio.MinioFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
