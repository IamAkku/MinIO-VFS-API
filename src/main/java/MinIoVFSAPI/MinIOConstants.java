package MinIoVFSAPI;

/**
 * Various constants used in the provider.  Currently just the default scheme is
 * declared here but that might change in the future.
 *
 * @author Akram Hussain
 */

public class MinIOConstants {

    /**
     * The default scheme used by this provider.
     *
     * This scheme is really S3's HTTP scheme with the HTTP protocol name
     * replaced by the following value.  This is done because Commons VFS is very
     * scheme aware and using the regular HTTP scheme would clash with the default
     * HTTP provider in many cases.
     *
     * MINIOSCHEME://<authority>/<containter>/<path>
     *
     * E.g. s3://s3.amazonaws.com/myTestContainer01/testFolder01/testSubFolder01/testFile01.txt
     *
     */

    public static final String MINIOSCHEME = "minio";
    public MinIOConstants() {
    }

}
