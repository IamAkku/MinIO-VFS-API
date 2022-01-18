package MinIoVFSAPI;

import io.minio.MinioClient;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * File-System object represents a connect to Minio via a single client.
 *
 * @author Akram Hussain
 */
public class MinIOFileSystem
        extends AbstractFileSystem
        implements FileSystem
{
    private static final Logger log = LoggerFactory.getLogger(MinIOFileSystem.class);

    private final MinioClient client;

    /**
     * The single client for interacting with Amazon S3.
     *
     * @return
     */
    protected MinioClient getClient()
    {
        return client;
    }

    protected MinIOFileSystem(final GenericFileName rootName, final MinioClient client,
                              final FileSystemOptions fileSystemOptions)
    {
        super(rootName, null, fileSystemOptions);
        this.client = client;
    }

    @Override
    protected FileObject createFile(AbstractFileName name) throws Exception
    {
        return new MinIOFileObject(name, this);
    }

    @Override
    protected void addCapabilities(Collection<Capability> caps)
    {
        caps.addAll(MinIOFileProvider.capabilities);
    }

}
