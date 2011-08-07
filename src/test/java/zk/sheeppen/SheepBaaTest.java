package zk.sheeppen;


import junit.framework.TestCase;

import org.apache.commons.lang.ArrayUtils;

public class SheepBaaTest extends TestCase {

    public void testCommandWithArgs() throws Exception {
        SheepBaa cmd = SheepBaa.parse("probe_tv.start@1,2,3");
        assertEquals("probe_tv", cmd.getName());
        assertEquals("start", cmd.getMethod());
        assertEquals(3, cmd.getArgs().length);
        assertTrue(ArrayUtils.isEquals(new String[] { "1", "2", "3" }, cmd.getArgs()));
    }

    public void testCommandNoArgs() throws Exception {
        SheepBaa cmd = SheepBaa.parse("probe_tv.start");
        assertEquals("probe_tv", cmd.getName());
        assertEquals("start", cmd.getMethod());
        assertTrue(ArrayUtils.isEquals(new String[] {}, cmd.getArgs()));
    }

    public void testCommandContainsWhitespaces() throws Exception {
        try {
            SheepBaa.parse("probe_tv.start@ 1, 2, 3");
        } catch (Exception e) {
            e.printStackTrace();
            fail("command must not contain the whitespaces.");
        }
    }
    
    public void testNoname() throws Exception {
        SheepBaa cmd = SheepBaa.parse(".start@1,2,3");
        assertEquals("global", cmd.getName());
        assertEquals("start", cmd.getMethod());
        assertEquals(3, cmd.getArgs().length);
        assertTrue(ArrayUtils.isEquals(new String[] { "1", "2", "3" }, cmd.getArgs()));
        
        cmd = SheepBaa.parse(".start");
        assertEquals("global", cmd.getName());
        assertEquals("start", cmd.getMethod());
        assertEquals(0, cmd.getArgs().length);
    }

}
