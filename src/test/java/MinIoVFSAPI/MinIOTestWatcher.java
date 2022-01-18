package MinIoVFSAPI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 *
 * @author Akram
 */
public class MinIOTestWatcher extends TestWatcher
{
    private static final Logger log
            = LoggerFactory.getLogger(MinIOTestWatcher.class);

    @Override
    protected void failed(Throwable e, Description description)
    {
        log.info(
                String.format("%s failed %s",
                        description.getDisplayName(), e.getMessage()));

        super.failed(e, description);
    }

    @Override
    protected void succeeded(Description description)
    {
        log.info(
                String.format("%s succeeded.",
                        description.getDisplayName()));

        super.succeeded(description);
    }
}

