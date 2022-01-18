package MinIoVFSAPI;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.Properties;
import junit.framework.Assert;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Akram
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MinIOClientTest {
    private static final Logger log = LoggerFactory.getLogger(MinIOClientTest.class);

    private Properties testProperties;

    public MinIOClientTest()
    {
    }

    @Rule
    public TestWatcher testWatcher = new MinIOTestWatcher();

    @Before
    public void setUp()
    {

        /**
         * Get the current test properties from a file so we don't hard-code
         * in our source code.
         */
       testProperties = MinIOTestProperties.GetProperties();

        try
        {
            /**
             * Setup the remote folders for testing
             */
            uploadFileSetup02();
        }
        catch (Exception ex)
        {
            log.debug("Error setting up remote folder structure.  Have you set the test001.properties file?", ex);
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @After
    public void tearDown() throws Exception
    {
        removeFileSetup02();
    }

    /**
     * Upload a single file to the test bucket.
     * @throws java.lang.Exception
     */
    @Test
    public void A001_uploadFile() throws Exception {
        final String endPoint = testProperties.getProperty("minio.host");
        final String accessKey = testProperties.getProperty("minio.access.id");
        final String secretKey = testProperties.getProperty("minio.access.secret");
        final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
        //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";

        String currFileNameStr;

        //Creating a temporary file to upload

        File temp = File.createTempFile("uploadFile01", ".tmp");
        try (FileWriter fw = new FileWriter(temp))
        {
            BufferedWriter bw = new BufferedWriter(fw);
            bw.append("testing...");
            bw.flush();
        }

        MinIOFileProvider currMinIO = new MinIOFileProvider();

        // Optional set endpoint
        //currMinIO.setEndpoint(endPoint);

        // Optional set region
        //currMinIo.setRegion(currRegion);

        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider("minio", currMinIO);
        currMan.addProvider("file", new DefaultLocalFileProvider());
        currMan.init();

        StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint, accessKey, secretKey);
        FileSystemOptions opts = new FileSystemOptions();
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", "minio", endPoint, bucketName, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        FileObject currFile2 = currMan.resolveFile(
                String.format("file://%s", temp.getAbsolutePath()));

        currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
        temp.delete();
    }

        /**
         * Download a previously uploaded file from the test bucket.
         * @throws Exception
         */
        @Test
        public void A002_downloadFile() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";
            String currFileNameStr;

            MinIOFileProvider currMinIO = new MinIOFileProvider();

            // Optional set endpoint
            //currMinIO.setEndpoint(endPoint);

            // Optional set region
            //currMinIO.setRegion(currRegion);

            File temp = File.createTempFile("downloadFile01", ".tmp");

            DefaultFileSystemManager currMan = new DefaultFileSystemManager();
            currMan.addProvider("minio", currMinIO);
            currMan.addProvider("file", new DefaultLocalFileProvider());
            currMan.init();

            StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint, accessKey, secretKey);
            FileSystemOptions opts = new FileSystemOptions();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

            currFileNameStr = "test01.tmp";
            String currUriStr = String.format("%s://%s/%s/%s", "minio", endPoint, bucketName, currFileNameStr);
            FileObject currFile = currMan.resolveFile(currUriStr, opts);

            String destStr = String.format("file://%s", temp.getAbsolutePath());
            FileObject currFile2 = currMan.resolveFile( destStr );

            log.info( String.format("copying '%s' to '%s'", currUriStr, destStr));

            currFile2.copyFrom(currFile, Selectors.SELECT_SELF);
        }

        @Test
        public void A003_exist() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";
            String currFileNameStr;

            MinIOFileProvider currMinIO = new MinIOFileProvider();

            DefaultFileSystemManager currMan = new DefaultFileSystemManager();
            currMan.addProvider("minio", currMinIO);
            currMan.init();

            StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint, accessKey, secretKey);
            FileSystemOptions opts = new FileSystemOptions();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

            currFileNameStr = "test01.tmp";

            String currUriStr = String.format("%s://%s/%s/%s", "minio", endPoint, bucketName, currFileNameStr);
            FileObject currFile = currMan.resolveFile(currUriStr, opts);


            log.info( String.format("exist() file '%s'", currUriStr));

            Boolean existRes = currFile.exists();
            Assert.assertTrue(existRes);


            currFileNameStr = "non-existant-file-2.tmp";
            currUriStr = String.format("%s://%s/%s/%s",
                    "minio", endPoint, bucketName, currFileNameStr);
            currFile = currMan.resolveFile(currUriStr, opts);

            log.info( String.format("exist() file '%s'", currUriStr));

            existRes = currFile.exists();
            Assert.assertFalse(existRes);
        }

        @Test
        public void A004_getContentSize() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";
            String currFileNameStr;

            MinIOFileProvider currMinIO = new MinIOFileProvider();

            DefaultFileSystemManager currMan = new DefaultFileSystemManager();
            currMan.addProvider("minio", currMinIO);
            currMan.init();

            StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint,accessKey,secretKey);
            FileSystemOptions opts = new FileSystemOptions();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

            currFileNameStr = "test01.tmp";
            String currUriStr = String.format("%s://%s/%s/%s",
                    "minio", endPoint, bucketName, currFileNameStr);
            FileObject currFile = currMan.resolveFile(currUriStr, opts);

            log.info( String.format("getContent() file '%s'", currUriStr));

            FileContent cont = currFile.getContent();
            long contSize = cont.getSize();

            Assert.assertTrue(contSize>0);

        }

        @Test
        public void A005_testContent() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";
            String currFileNameStr;

            MinIOFileProvider currMinIO = new MinIOFileProvider();

            DefaultFileSystemManager currMan = new DefaultFileSystemManager();
            currMan.addProvider("minio", currMinIO);
            currMan.init();

            StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint,accessKey,secretKey);
            FileSystemOptions opts = new FileSystemOptions();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

            currFileNameStr = "file05";
            String currUriStr = String.format("%s://%s/%s/%s",
                    "minio", endPoint, bucketName, currFileNameStr);
            FileObject currFile = currMan.resolveFile(currUriStr, opts);

            FileContent content = currFile.getContent();
            long size = content.getSize();
            Assert.assertTrue( size >= 0);

            long modTime = content.getLastModifiedTime();
            Assert.assertTrue(modTime>0);
        }

        /**
         * Delete a previously uploaded file.
         *
         * @throws Exception
         */
        @Test
        public void A006_deleteFile() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";
            String currFileNameStr;

            MinIOFileProvider currMinIO = new MinIOFileProvider();

            DefaultFileSystemManager currMan = new DefaultFileSystemManager();
            currMan.addProvider("minio", currMinIO);
            currMan.init();

            StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint,accessKey,secretKey);
            FileSystemOptions opts = new FileSystemOptions();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

            currFileNameStr = "test01.tmp";
            String currUriStr = String.format("%s://%s/%s/%s",
                    "minio", endPoint,bucketName, currFileNameStr);
            FileObject currFile = currMan.resolveFile(currUriStr, opts);

            log.info( String.format("deleting '%s'", currUriStr));

            Boolean delRes = currFile.delete();
            Assert.assertTrue(delRes);
        }

        /**
         * By default FileObject.getChildren() will use doListChildrenResolved() if available
         *
         * @throws Exception
         */
        @Test
        public void A007_listChildren() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";

            DefaultFileSystemManager currMan = new DefaultFileSystemManager();
            MinIOFileProvider currMinIO = new MinIOFileProvider();

            // Optional set endpoint
            //currMinIO.setEndpoint(endPoint);

            // Optional set region
            //currMinIO.setRegion(currRegion);

            currMan.addProvider("minio", currMinIO );
            currMan.init();

            StaticUserAuthenticator auth = new StaticUserAuthenticator(endPoint,accessKey,secretKey);
            FileSystemOptions opts = new FileSystemOptions();
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

            String currFileNameStr = "uploadFile02";
            String currUriStr = String.format("%s://%s/%s/%s",
                    "minio", endPoint,bucketName, currFileNameStr);
            FileObject currFile = currMan.resolveFile(currUriStr, opts);

            FileObject[] currObjs = currFile.getChildren();
            for(FileObject obj : currObjs)
            {
                FileName currName = obj.getName();
                Boolean res = obj.exists();
                FileType ft = obj.getType();

                log.info( String.format("\nNAME.PATH : '%s'\nEXISTS : %b\nTYPE : %s\n\n",
                        currName.getPath(), res, ft));
            }
        }

        public void uploadFileSetup02() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";

            File temp = MinIOTestUtils.createTempFile("uploadFile02", "tmp", "File 01");
            MinIOTestUtils.uploadFile(accessKey, endPoint, secretKey, bucketName, temp.toPath(),
                    Paths.get("uploadFile02/dir01/file01"));
            temp.delete();

            temp = MinIOTestUtils.createTempFile("uploadFile02", "tmp", "File 02");
            MinIOTestUtils.uploadFile(accessKey, endPoint, secretKey, bucketName, temp.toPath(),
                    Paths.get("uploadFile02/dir01/file02"));
            temp.delete();

            temp = MinIOTestUtils.createTempFile("uploadFile02", "tmp", "File 03");
            MinIOTestUtils.uploadFile(accessKey, endPoint, secretKey, bucketName, temp.toPath(),
                    Paths.get("uploadFile02/dir02/file03"));
            temp.delete();

            temp = MinIOTestUtils.createTempFile("uploadFile02", "tmp", "File 04");
            MinIOTestUtils.uploadFile(accessKey, endPoint, secretKey, bucketName, temp.toPath(),
                    Paths.get("uploadFile02/file04"));
            temp.delete();

            temp = MinIOTestUtils.createTempFile("uploadFile02", "tmp", "File 05");
            MinIOTestUtils.uploadFile(accessKey, endPoint, secretKey, bucketName, temp.toPath(),
                    Paths.get("file05"));
            temp.delete();

            temp = MinIOTestUtils.createTempFile("uploadFile02", "tmp", "File 06");
            MinIOTestUtils.uploadFile(accessKey, endPoint, secretKey, bucketName, temp.toPath(),
                    Paths.get("uploadFile02/dir02/file06"));
            temp.delete();
        }

        public void removeFileSetup02() throws Exception
        {
            final String endPoint = testProperties.getProperty("minio.host");
            final String accessKey = testProperties.getProperty("minio.access.id");
            final String secretKey = testProperties.getProperty("minio.access.secret");
            final String bucketName = testProperties.getProperty("minio.test0001.bucket.name");
            //final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";

            MinIOTestUtils.deleteFile(accessKey,endPoint,secretKey,bucketName,
                    Paths.get("uploadFile02/dir01/file01"));

            MinIOTestUtils.deleteFile(accessKey,endPoint,secretKey,bucketName,
                    Paths.get("uploadFile02/dir01/file02"));

            MinIOTestUtils.deleteFile(accessKey,endPoint,secretKey,bucketName,
                    Paths.get("uploadFile02/dir02/file03"));

            MinIOTestUtils.deleteFile(accessKey,endPoint,secretKey,bucketName,
                    Paths.get("uploadFile02/file04"));

            MinIOTestUtils.deleteFile(accessKey,endPoint,secretKey,bucketName,
                    Paths.get("file05"));

            MinIOTestUtils.deleteFile(accessKey,endPoint,secretKey,bucketName,
                    Paths.get("uploadFile02/dir02/file06"));
        }
    }
