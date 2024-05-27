package com.bky373.springkafkaplayground;

public class ThreadSupport {

    public static String getName() {
        return Thread.currentThread()
                     .getName();
    }

    public static void printCurrentName() {
        System.out.println("# Thread Name = "+ getName());
    }
}
