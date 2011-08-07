package zk.sheeppen.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SplitInputStream extends FilterInputStream {

    private int splitSize = 1024;
    private byte[] space;

    public SplitInputStream(InputStream in) {
        super(in);
        this.space = new byte[splitSize];
    }

    public byte[] readNextSplit() throws IOException {
        int count = in.read(space, 0, splitSize);
        if (count == -1)
            return null;
        if (count == splitSize)
            return space;
        byte[] data = new byte[count];
        System.arraycopy(space, 0, data, 0, count);
        return data;
    }

}
