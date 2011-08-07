package zk.sheeppen.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.lang.ArrayUtils;

public class SpliteInputStreamTest extends TestCase {

    public void testSplitInputStream() throws Exception {
        Random random = new Random();
        byte[] data = new byte[4096 + random.nextInt(1024)];
        random.nextBytes(data);

        SplitInputStream splitInputStream = new SplitInputStream(new ByteArrayInputStream(data));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (true) {
            byte[] s = splitInputStream.readNextSplit();
            if (s == null)
                break;
            out.write(s);
        }

        /* merge the split data should equal to the origin data. */
        assertTrue(ArrayUtils.isEquals(data, out.toByteArray()));
    }

}
