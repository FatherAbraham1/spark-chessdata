package fr.st.spark.chessdata;

import org.apache.spark.api.java.JavaSparkContext;


class ChessDataAS {

    public static void main(String[] arg) {

        JavaSparkContext sc = new JavaSparkContext("local[16]", "chess");

        long start = System.currentTimeMillis();

        sc.textFile(PGNPath.PGN_PATH)
                .filter(line -> line.startsWith("[Result ") && line.contains("-"))
                .map(res -> res.substring(res.indexOf("\"") + 1, res.indexOf("-")))
                .filter(res -> res.equals("0") || res.equals("1") || res.equals("1/2"))
                .countByValue()
                .entrySet()
                .stream()
                .forEach(s -> System.out.println(s.getKey() + " -> " + s.getValue()));

        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration: " + duration + " ms");
    }
}
