package data_generator_simulator;

public class SampleUtils {
    public static final long PROGRESS_REPORTING_INTERVAL = 5;

    public static String setStringParameters(String par_name, String def_str) {
        String ret_value = def_str;
        String temp_env_str = System.getenv(par_name);
        if (temp_env_str != null && temp_env_str.length() > 0) {
            ret_value = temp_env_str;
        }
        
        return ret_value;
    }

    public static int setIntegerParameters(String par_name, int def_int) {
        int ret_value = def_int;
        String temp_env_str = System.getenv(par_name);
        if (temp_env_str != null && temp_env_str.length() > 0) {
            ret_value = Integer.parseInt(temp_env_str);
        }
        
        return ret_value;
    }

}
