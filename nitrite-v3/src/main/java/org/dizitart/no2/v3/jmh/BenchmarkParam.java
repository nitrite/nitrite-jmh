package org.dizitart.no2.v3.jmh;

import org.apache.commons.text.RandomStringGenerator;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * @author Anindya Chatterjee
 */
public class BenchmarkParam {
    public volatile static String TMP = System.getProperty("java.io.tmpdir");

    public final static int FORKS = 1;

    public final static int WARMUPS = 3;

    public final static int ITERATIONS = 3;

    public final static int MILLISECONDS = 1500;

    public final static Random RANDOM;

    public final static RandomStringGenerator GENERATOR;

    public final static String CREATE_TABLE_STATEMENT =
            "CREATE TABLE IF NOT EXISTS arbitrary ("
                    + "  id INTEGER PRIMARY KEY,"
                    + "  text TEXT NOT NULL,"
                    + "  number1 REAL NOT NULL,"
                    + "  number2 REAL NOT NULL,"
                    + "  index1 INTEGER NOT NULL,"
                    + "  flag1 INTEGER NOT NULL,"
                    + "  flag2 INTEGER NOT NULL);";

    public final static String CREATE_INDEX1_STATEMENT =
            "CREATE INDEX index1_idx ON arbitrary(index1)";

    public final static String INSERT_TABLE_STATEMENT = "INSERT INTO arbitrary(id,text,number1,number2,index1,flag1,flag2) " +
            " VALUES (?,?,?,?,?,?,?)";

    public final static String SELECT_INDEX1_STATEMENT =
            "SELECT * FROM arbitrary WHERE index1=? and number1=?";
//            "SELECT * FROM arbitrary WHERE index1=?";

    static {
        try {
            RANDOM = SecureRandom.getInstance("NativePRNGNonBlocking");
            GENERATOR = new RandomStringGenerator.Builder().usingRandom(RANDOM::nextInt).build();
        } catch (SecurityException | NoSuchAlgorithmException x) {
            throw new RuntimeException(x);
        }
    }
}
