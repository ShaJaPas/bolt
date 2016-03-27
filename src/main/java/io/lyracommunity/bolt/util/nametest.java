package io.lyracommunity.bolt.util;

import java.util.Arrays;
import java.util.List;

/**
 * Created by keen on 27/03/16.
 */
public class nametest {

    final static List<String> l1 = Arrays.asList(
            "Opus",
            "Invent",
            "Reverie",
            "Emprise",
            "Lyra",
            "Atlas",
            "Estuary"
    );

    final static List<String> l2 = Arrays.asList(
            "Society",
            "Community",
            "Collective",
            "Home",
            "House",
            "Vision",
            "Dream"
    );

    public static void main(String[] args) {
        for (String s1 : l2) {
            for (String s2 : l1) {
                System.out.println(s2 + " " + s1);
            }
        }
    }

}
