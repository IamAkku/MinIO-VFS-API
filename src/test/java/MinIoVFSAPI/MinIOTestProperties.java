package MinIoVFSAPI;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Akram
 */

public class MinIOTestProperties
{
    private static final Logger log = LoggerFactory.getLogger(MinIOTestProperties.class);

    public static Properties GetProperties()
    {
        Properties testProperties = new Properties();
        try
        {
            testProperties.load(MinIOTestProperties.class
                    .getClassLoader()
                    .getResourceAsStream("test001.properties"));
        }
        catch (IOException ex)
        {
            log.error("Error loading properties file", ex);
        }

        return testProperties;
    }
}

