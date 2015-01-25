package fr.st.spark.chessdata;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaSparkContext;

class ChessDataTextSum {

    private final static byte[] RESULT = "[Result".getBytes();

    private final static int[] WHITE = {1, 0, 0};
    private final static int[] BLACK = {0, 1, 0};
    private final static int[] DRAW = {0, 0, 1};
    private final static int[] ERROR = {0, 0, 0};

    public static void main(String[] arg) {

        JavaSparkContext sc = new JavaSparkContext("local[16]", "chess");

        StopWatch sw = new StopWatch();
        sw.start();

        int[] count = sc.hadoopFile(PGNPath.PGN_PATH, TextInputFormat.class, LongWritable.class, Text.class, 2)
                .map(t2 -> t2._2())
                .filter(text -> startsWithResult(text))
                .map(text -> extractBlackValue(text))
                .reduce(ChessDataTextSum::sum);

        sw.stop();

        System.out.println(count[0] + " " + count[1] + " " + count[2]);
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

    private static int[] extractBlackValue(Text text) {
        int[] array;
        byte[] value = text.getBytes();
        byte c = value[text.getLength() - 3]; // 0 for white; 1 for black; 2 for draw

        if (c == '1') {
            array = BLACK;
        } else if (c == '0') {
            array = WHITE;
        } else if (c == '2') {
            array = DRAW;
        } else {
            array = ERROR;
        }

        return array;
    }

    private static int[] sum(int[] a, int[] b) {
        int[] c = new int[3];

        c[0] = a[0] + b[0];
        c[1] = a[1] + b[1];
        c[2] = a[2] + b[2];

        return c;
    }

}
