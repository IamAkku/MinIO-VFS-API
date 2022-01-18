package MinIoVFSAPI;

import io.minio.errors.*;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;


    /**
     * Wrap an output stream for Minio stream upload.  Which unfortunately uses an
     * InputStream.
     *
     * This OutputStream buffers all data to a local file then automatically uploads
     * it to minio after <code>close()</code> is called.
     *
     * @author Akram
     */
    public final class MinIOOutputStream extends OutputStream
    {
        private final File tempFile;
        private final OutputStream tempFileStream;
        private final MinIOFileObject fileObject;

        public File getTempFile()
        {
            return tempFile;
        }

        public MinIOOutputStream(MinIOFileObject fo) throws IOException
        {
            super();

            tempFile = File.createTempFile("bin", "bin");
            tempFile.deleteOnExit();

            tempFileStream = new BufferedOutputStream(new FileOutputStream(tempFile));

            fileObject = fo;
        }

        @Override
        public void write(int i) throws IOException
        {
            tempFileStream.write(i);
        }

        @Override
        public void close() throws IOException
        {
            tempFileStream.close();

            // Upload tempFile
            try {
                fileObject.putObject(tempFile);
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (InvalidBucketNameException e) {
                e.printStackTrace();
            } catch (InsufficientDataException e) {
                e.printStackTrace();
            } catch (ErrorResponseException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            } catch (InvalidResponseException e) {
                e.printStackTrace();
            } catch (XmlParserException e) {
                e.printStackTrace();
            } catch (InternalException e) {
                e.printStackTrace();
            }
            tempFile.delete();
        }

        @Override
        public void flush() throws IOException
        {
            tempFileStream.flush();
        }

        @Override
        public void write(byte[] bytes, int i, int i1) throws IOException
        {
            tempFileStream.write(bytes, i, i1);
        }

        @Override
        public void write(byte[] bytes) throws IOException
        {
            tempFileStream.write(bytes);
        }

        @Override
        public String toString()
        {
            return super.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            return super.equals(o);
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }
    }
