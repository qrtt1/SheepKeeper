package zk.sheeppen;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * Baa is a sheep keeper command protocol. 
 * Example:
 * 
 * <li> sheep1.start
 * <li> sheep2.start@arg1,arg2,arg3
 * <li> blacksheep1.start@3
 * 
 * @author qrtt1
 */
public class SheepBaa {

    private static Pattern pattern = Pattern.compile("([0-9_\\w]*)\\.([0-9_\\w]+)(@(.+))?");
    private String name;
    private String method;
    private String args[] = new String[0];

    private SheepBaa() {
    }

    public static SheepBaa parse(String input) {
        String command = StringUtils.trimToEmpty(input);
        if (command == null || StringUtils.isEmpty(command))
            throw new IllegalArgumentException("null or empty command");
        if(StringUtils.contains(command, "\t \r\n"))
                throw new IllegalArgumentException("whitespaces is not allowed");
        
        Matcher matcher = pattern.matcher(StringUtils.trimToEmpty(command));
        if (!matcher.matches())
            throw new IllegalArgumentException("cannot parse command: " + command);
        
        SheepBaa cmd = new SheepBaa();
        cmd.name = matcher.group(1);
        cmd.method = matcher.group(2);
        String args = matcher.group(4);
        if (args != null) {
            ArrayList<String> argList = new ArrayList<String>();
            for (String arg : args.split(",")) {
                if (StringUtils.isEmpty(arg))
                    throw new IllegalArgumentException("cannot have empty argument value: "
                            + command);
                argList.add(arg);
            }
            cmd.args = argList.toArray(new String[0]);
        }

        if (StringUtils.isEmpty(cmd.name)) {
            cmd.name = "global";
        }
        return cmd;
    }

    public String getName() {
        return name;
    }

    public String getMethod() {
        return method;
    }

    public String[] getArgs() {
        return args;
    }
    
    public boolean hasArgs() {
        return args != null && args.length > 0;
    }

}
