package org.embulk.input.minio;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.Result;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.messages.Item;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class MinioFileInputPlugin
        implements FileInputPlugin
{
    private final Logger log = Exec.getLogger(MinioFileInputPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("bucket")
        public String getBucket();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("last_path")
        @ConfigDefault("null")
        public Optional<String> getLastPath();

        @Config("access_key_id")
        @ConfigDefault("null")
        public Optional<String> getAccessKeyId();

        @Config("incremental")
        @ConfigDefault("true")
        public boolean getIncremental();

        // TODO timeout, ssl, etc

        public FileList getFiles();
        public void setFiles(FileList files);

        @Config("endpoint")
        @ConfigDefault("null")
        public String getEndpoint();

        @Config("access_key_id")
        String getAccessKeyId();

        @Config("secret_access_key")
        String getSecretAccessKey();

/*

        @Config("access_key_id")
        @ConfigDefault("null")
        Optional<String> getAccessKeyId();

        @Config("secret_access_key")
        @ConfigDefault("null")
        Optional<String> getSecretAccessKey();

        @Config("session_token")
        @ConfigDefault("null")
        Optional<String> getSessionToken();

        @Config("profile_file")
        @ConfigDefault("null")
        Optional<LocalFile> getProfileFile();

        @Config("profile_name")
        @ConfigDefault("null")
        Optional<String> getProfileName();
*/

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class());

        // list files recursively
        task.setFiles(listFiles(task));

        // number of processors is same with number of files
        return resume(task.dump(), task.getFiles().getTaskCount(), control);
    }


    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class());

        // validate task
        newMinioClient(task);

        control.run(taskSource, taskCount);

        // build next config
        ConfigDiff configDiff = Exec.newConfigDiff();

        // last_path
        if (task.getIncremental()) {
            configDiff.set("last_path", task.getFiles().getLastPath(task.getLastPath()));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    protected MinioClient newMinioClient(PluginTask task)
            throws InvalidEndpointException,InvalidPortException
    {
        return new MinioClient(task.getEndpoint(),task.getAccessKeyId(),task.getSecretAccessKey());
    }


    private FileList listFiles(PluginTask task)
    {
        try {
            MinioClient client = newMinioClient(task);
            String bucketName = task.getBucket();

            if (task.getPathPrefix().equals("/")) {
                log.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
            }

            FileList.Builder builder = new FileList.Builder(task);
            listS3FilesByPrefix(builder, client, bucketName,
                    task.getPathPrefix(), task.getLastPath());
            return builder.build();
        }
        catch (InvalidEndpointException|InvalidPortException ex) {
            throw new ConfigException(ex);
        }
    }

    /**
     * Lists S3 filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    public static void listS3FilesByPrefix(FileList.Builder builder,
            MinioClient client, String bucketName,
            String prefix, Optional<String> lastPath)
    {
        // TODO last_path support.
        Iterable<Result<Item>> myObjects;
        client.listObjects(bucketName,prefix,true);
        for (Result<Item> result : myObjects) {
            Item item = result.get();
            if( item.size() > 0 )
            {
                builder.add(item.objectName(),item.size());
                // TODO last path
                if (!builder.needsMore()) {
                    return;
                }
            }
        }
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class());
        return new MinioInput(task, taskIndex);
    }

    @VisibleForTesting
    static class S3InputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private final Logger log = Exec.getLogger(S3InputStreamReopener.class);

        private final MinioClient client;
        private final long contentLength;
        private final String path;

        public S3InputStreamReopener(MinioClient client, String path, long contentLength)
        {
            this.client = client;
            this.path = path;
            this.contentLength = contentLength;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            try {
                return retryExecutor()
                        .withRetryLimit(3)
                        .withInitialRetryWait(500)
                        .withMaxRetryWait(30*1000)
                        .runInterruptible(new RetryExecutor.Retryable<InputStream>() {
                            @Override
                            public InputStream call() throws InterruptedIOException
                            {
                                log.warn(String.format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
                                request.setRange(offset, contentLength - 1);  // [first, last]
                                return client.getObject(request).getObjectContent();
                            }

                            @Override
                            public boolean isRetryableException(Exception exception)
                            {
                                return true;  // TODO
                            }

                            @Override
                            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                    throws RetryExecutor.RetryGiveupException
                            {
                                String message = String.format("S3 GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                        retryCount, retryLimit, retryWait/1000, exception.getMessage());
                                if (retryCount % 3 == 0) {
                                    log.warn(message, exception);
                                } else {
                                    log.warn(message);
                                }
                            }

                            @Override
                            public void onGiveup(Exception firstException, Exception lastException)
                                    throws RetryExecutor.RetryGiveupException
                            {
                            }
                        });
            } catch (RetryExecutor.RetryGiveupException ex) {
                Throwables.propagateIfInstanceOf(ex.getCause(), IOException.class);
                throw Throwables.propagate(ex.getCause());
            } catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }
    }

    public class MinioInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public MinioInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort() { }

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close() { }
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private MinioClient client;
        private final String bucket;
        private final Iterator<String> iterator;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newMinioClient(task);
            this.bucket = task.getBucket();
            this.iterator = task.getFiles().get(taskIndex).iterator();
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (!iterator.hasNext()) {
                return null;
            }
            String name = iterator.next();
            ObjectStat stat = client.statObject(bucket,name);
            InputStream stream = client.getObject(bucket, iterator.next());
            return new ResumableInputStream(stream, new S3InputStreamReopener(client, name, stat.length()));
        }

        @Override
        public void close() { }
    }
}
