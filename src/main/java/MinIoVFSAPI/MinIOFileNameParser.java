package MinIoVFSAPI;

import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for defining / parsing a provided FileName object.
 *
 * This name should adhere to a URL structure, complete with an 'authority'
 *
 * &lt;scheme&gt;://&lt;host_or_authority&gt;/&lt;container&gt;/&lt;file_path&gt;
 * E.g. s3://aws.amazon.com/myBucket/path/to/file.txt
 *
 */
public class MinIOFileNameParser extends URLFileNameParser
{
    private static final Logger log = LoggerFactory.getLogger(MinIOFileNameParser.class);

    private static final MinIOFileNameParser INSTANCE = new MinIOFileNameParser();

    public MinIOFileNameParser()
    {
        super(80);
    }

    public static FileNameParser getInstance()
    {
        return INSTANCE;
    }
}
