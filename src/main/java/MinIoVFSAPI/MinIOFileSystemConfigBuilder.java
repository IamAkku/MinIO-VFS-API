package MinIoVFSAPI;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Akram Hussain
 */
public class MinIOFileSystemConfigBuilder extends FileSystemConfigBuilder
{
    private static final Logger log = LoggerFactory.getLogger(MinIOFileSystemConfigBuilder.class);
    private static final MinIOFileSystemConfigBuilder BUILDER = new MinIOFileSystemConfigBuilder();

    @Override
    protected Class<? extends FileSystem> getConfigClass()
    {
        return MinIOFileSystem.class;
    }

    protected MinIOFileSystemConfigBuilder(String prefix)
    {
        super(prefix);
    }

    private MinIOFileSystemConfigBuilder()
    {
        super("azure.");
    }

    public static MinIOFileSystemConfigBuilder getInstance()
    {
        return BUILDER;
    }

    /**
     * Sets the user authenticator to get authentication informations.
     * @param opts The FileSystemOptions.
     * @param userAuthenticator The UserAuthenticator.
     * @throws FileSystemException if an error occurs setting the UserAuthenticator.
     */
    public void setUserAuthenticator(FileSystemOptions opts, UserAuthenticator userAuthenticator)
            throws FileSystemException
    {
        setParam(opts, "userAuthenticator", userAuthenticator);
    }

    /**
     * @see #setUserAuthenticator
     * @param opts The FileSystemOptions.
     * @return The UserAuthenticator.
     */
    public UserAuthenticator getUserAuthenticator(FileSystemOptions opts)
    {
        return (UserAuthenticator) getParam(opts, "userAuthenticator");
    }
}
