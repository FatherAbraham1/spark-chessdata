package fr.st.spark.chessdata;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaSparkContext;

class ChessDataText {

    private final static byte[] RESULT = "[Result".getBytes();

    public static void main(String[] arg) {

        JavaSparkContext sc = new JavaSparkContext("local[16]", "chess");

        StopWatch sw = new StopWatch();
        sw.start();

        sc.hadoopFile(PGNPath.PGN_PATH, TextInputFormat.class, LongWritable.class, Text.class, 2)
                .filter(t2 -> startsWithResult(t2._2()))
                .map(t2 -> extractBlackValue(t2._2()))
                .countByValue()
                .entrySet()
                .stream()
                .filter(s -> s.getKey() == 0 || s.getKey() == 1 || s.getKey() == 2)
                .forEach(s -> System.out.println(s.getKey() + " -> " + s.getValue()));

        sw.stop();
        System.out.println("Duration : " + sw.toString());

    }

    private static boolean startsWithResult(Text text) {
        byte[] from = text.getBytes();
        int pc = RESULT.length;

        if (text.getLength() < pc) {
            return false;
        }

        while (--pc >= 0) {
            if (RESULT[pc] != from[pc]) {
                return false;
            }
        }

        return true;
    }

    private static int extractBlackValue(Text text) {
        byte[] value = text.getBytes();
        byte c = value[text.getLength() - 3]; // 0 for white; 1 for black; 2 for draw

        return c - '0';
    }
}
