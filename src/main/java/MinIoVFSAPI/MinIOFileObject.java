package MinIoVFSAPI;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.RemoveObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.GetObjectArgs.Builder;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidBucketNameException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.ObjectMetadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.URLFileName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 *
 * @author Akram Hussain
 */
public class MinIOFileObject extends AbstractFileObject {
    private static final Logger log = LoggerFactory.getLogger(MinIOFileObject.class);

    private final MinIOFileSystem fileSystem;
    private InputStream currBlob;
    private ObjectMetadata currBlobProperties;


    /**
     * Creates a new FileObject for use with a remote minio file or folder.
     *
     * @param name
     * @param fileSystem
     */
    protected MinIOFileObject(AbstractFileName name, MinIOFileSystem fileSystem) {
        super(name, fileSystem);
        this.fileSystem = fileSystem;
    }

    /**
     * Convenience method that returns the container ( i.e. "bucket ) and path from the current URL.
     *
     * @return A tuple containing the bucket name and the path.
     */
    private Pair<String, String> getContainerAndPath() {
        ImmutablePair res = null;

        try {
            URLFileName currName = (URLFileName) this.getName();
            String currPathStr = currName.getPath();
            currPathStr = StringUtils.stripStart(currPathStr, "/");
            if (StringUtils.isBlank(currPathStr)) {
                log.warn(String.format("getContainerAndPath() : Path '%s' does not appear to be valid", currPathStr));
                return null;
            }

            // Deal with the special case of the container root.
            if (!StringUtils.contains(currPathStr, "/")) {
                return new ImmutablePair(currPathStr, "/");
            }

            String[] resArray = StringUtils.split(currPathStr, "/", 2);
            res = new ImmutablePair(resArray[0], resArray[1]);
        } catch (Exception var5) {

            log.error(String.format("getContainerAndPath() : Path does not appear to be valid"), var5);
        }

        return res;
    }

    /**
     * Callback used when this FileObject is first used.  We connect to the remote
     * server and check early so we can 'fail-fast'.  If there are no issues then
     * this FileObject can be used.
     *
     * @throws Exception
     */
    @Override
    protected void doAttach() throws Exception {
        Pair path = this.getContainerAndPath();


        try {
            // Check the container.  Force a network call so we can fail-fast
            if (this.objectExists((String) path.getLeft(), (String) path.getRight())) {
                byte[] bytes = this.fileSystem.getClient().getObject((GetObjectArgs) ((Builder) ((Builder) GetObjectArgs.builder().bucket((String) path.getLeft())).object((String) path.getRight())).build()).readAllBytes();
                this.currBlob = new ByteArrayInputStream(bytes);
            } else {
                this.currBlob = null;
            }

        } catch (RuntimeException var3) {
            log.error(String.format("doAttach() Exception for '%s' : '%s'", path.getLeft(), path.getRight()), var3);
            throw var3;
        }
    }

    private boolean objectExists(String cont, String path) {
        boolean res = false;

        try {
            this.fileSystem.getClient().getObject((GetObjectArgs) ((Builder) ((Builder) GetObjectArgs.builder().bucket(cont)).object(path)).build());
            res = true;
        } catch (InsufficientDataException | InternalException | InvalidBucketNameException | InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IOException | ErrorResponseException var6) {
            String errorCode = var6.getMessage();

            log.error(" error while checking the minio object");

        }

        return res;
    }

    /**
     * Callback for checking the type of the current FileObject.  Typically can
     * be of type...
     * FILE for regular remote files
     * FOLDER for regular remote containers
     * IMAGINARY for a path that does not exist remotely.
     *
     * @return
     * @throws Exception
     */
    @Override
    protected FileType doGetType() throws Exception {
        Pair<String, String> path = this.getContainerAndPath();
        FileType res;

        if (this.objectExists((String) path.getLeft(), (String) path.getRight())) {
            res = FileType.FILE;
        } else {
            // Blob Service does not have folders.  Just files with path separators in
            // their names.

            // Here's the trick for folders.
            //
            // Do a listing on that prefix.  If it returns anything, after not
            // existing, then it's a folder.
            String prefix = (String) path.getRight();
            if (!prefix.endsWith("/")) {
                // We need folders ( prefixes ) to end with a slash
                prefix = prefix + "/";
            }

            Iterable blobs;
            if (prefix.equals("/")) {
                // Special root path case. List the root blobs with no prefix

                blobs = this.fileSystem.getClient().listObjects((ListObjectsArgs) ((io.minio.ListObjectsArgs.Builder) ListObjectsArgs.builder().bucket((String) path.getLeft())).build());
            } else {
                blobs = this.fileSystem.getClient().listObjects((String) path.getLeft(), prefix);
            }
            //to do
            if (!blobs.iterator().hasNext()) {
                res = FileType.IMAGINARY;
            } else {
                res = FileType.FOLDER;
            }
        }

        return res;
    }


    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br />
     *
     * @return a possible empty String array if the file is a directory or null or an exception if the
     * file is not a directory or can't be read.
     * @throws Exception if an error occurs.
     */
    @Override
    protected String[] doListChildren() throws Exception {
        return new String[0];
    }


    /**
     * Upload a local file to Minio Bucket.
     *
     * @param f File object from the local file-system to be uploaded to Minio Bucket
     */
    public void putObject(File f) throws ServerException, InvalidBucketNameException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        Pair<String, String> path = this.getContainerAndPath();
        UploadObjectArgs args = (UploadObjectArgs) ((io.minio.UploadObjectArgs.Builder) ((io.minio.UploadObjectArgs.Builder) UploadObjectArgs.builder().bucket((String) path.getLeft())).object((String) path.getRight())).filename(f.getPath()).build();
        this.fileSystem.getClient().uploadObject(args);
    }


    /**
     * Get an InputStream for reading the content of this File Object.
     *
     * @return The InputStream object for reading.
     * @throws Exception
     */
    @Override
    protected InputStream doGetInputStream() throws Exception {
        return this.currBlob;
    }

    /**
     * Callback for handling delete on this File Object
     *
     * @throws Exception
     */
    @Override
    protected void doDelete() throws Exception {
        Pair<String, String> path = this.getContainerAndPath();

        // Purposely use the more restrictive delete() over deleteIfExists()

        this.fileSystem.getClient().removeObject((RemoveObjectArgs) ((io.minio.RemoveObjectArgs.Builder) ((io.minio.RemoveObjectArgs.Builder) RemoveObjectArgs.builder().bucket((String) path.getLeft())).object((String) path.getRight())).build());

    }

    /**
     * Callback for handling create folder requests.  Since there are no folders
     * in Minio this call is ingored.
     *
     * @throws Exception
     */
    @Override
    protected void doCreateFolder() throws Exception {
        log.info(String.format("doCreateFolder() called."));
    }

    @Override
    protected long doGetContentSize() throws Exception {
        return (long) this.currBlob.read();

    }

    /**
     * Used for creating folders.  It's not used since S3 does not have
     * the concept of folders.
     *
     * @throws FileSystemException
     */
    @Override
    public void createFolder() throws FileSystemException {
        log.info(String.format("createFolder() called."));
    }

    /**
     * Callback for getting an OutputStream for writing into Minio Bucket
     *
     * @param bAppend bAppend true if the file should be appended to, false if it should be overwritten.
     * @return An OutputStream for writing into Minio Bucket.
     * @throws Exception
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
        OutputStream res = new MinIOOutputStream(this);

        return res;
    }

    /**
     * Callback for use when detaching this File Object from Minio Bucket.
     * <p>
     * The File Object should be reusable after <code>attach()</code> call.
     *
     * @throws Exception
     */
    @Override
    protected void doDetach() throws Exception {
        this.currBlob = null;
        this.currBlobProperties = null;
    }


    /**
     * Callback for handling the <code>getLastModifiedTime()</code> Commons VFS API call.
     *
     * @return Time since the file has last been modified
     * @throws Exception
     */
    @Override
    protected long doGetLastModifiedTime() throws Exception {
        File target = new File("test2.tmp");
        OutputStream outStream = new FileOutputStream(target);
        outStream.write(currBlob.readAllBytes());
        return target.lastModified();
    }

}
