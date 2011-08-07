package zk.sheeppen.sheep;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

public class Path implements Iterable<String>, Iterator<String> {

    private String[] path;
    private StringBuffer sb = new StringBuffer();
    private int index = 0;
    private boolean root;

    public Path(String path) {
        if (path != null && path.length() != 0) {
            this.path = path.split("/");
            if(path.trim().charAt(0) == '/' && index == 0 && this.path.length == 0)
            {
                root = true;
            }
        }
    }

    @Override
    public Iterator<String> iterator() {
        return this;
    }

    @Override
    public synchronized boolean hasNext() {
        if (path == null)
            return false;
        if(root && index ==0)
            return true;
        return index < path.length;
    }

    @Override
    public synchronized String next() {
        if (root && index == 0) {
            index++;
            return "/";
        }
        if (path != null && index <= path.length -1) {
            try {
                if ("".equals(path[index].trim())) {
                    index ++;
                    return next();
                }
                sb.append("/");
                sb.append(path[index]);
                index++;
                return sb.toString();
            } catch (Exception e) {
                index ++;
                return next();
            }
        }
        return null;
    }
    
    public static String normalize(String path)
    {
        StringBuffer sb = new StringBuffer();
        for (String p : new Path(path)) {
            sb.setLength(0);
            sb.append(p);
        }
        if(sb.length() == 0)
        {
            return null;
        }
        return sb.toString();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Path object is readonly.");
    }
    
    public static String rebase(String base, String refPath)
    {
        if(refPath == null)
            return null;
        StringBuffer sb = new StringBuffer();
        for (String token : refPath.split("/")) {
            if (StringUtils.isNotEmpty(token)) {
                sb.setLength(0);
                sb.append(StringUtils.trimToEmpty(token));
            }
        }
        if(sb.length() != 0)
            return Path.normalize(base) + "/" + sb.toString();
        return null;
    }

}
