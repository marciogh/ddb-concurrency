package com.marciogh;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;

public class CommGenerator implements Runnable {

    static List<String> customers;
    public static volatile Integer counter = 0;
    private Random random = new Random();

    public CommGenerator(List<String> customers) {
        CommGenerator.counter = 0;
        CommGenerator.customers = customers;
    }

    @Override
    public void run() {
        while (CommGenerator.counter < 1000) {
            System.out.println("Generator count: " + CommGenerator.counter);
            int randCustomerIndex = random.nextInt(customers.size());
            long now = LocalDateTime.now().toInstant(ZoneOffset.of("Z")).toEpochMilli();
            long randFuture = (long) (Math.random() * 10000) + now;
            System.out.println(
                    "generating: " + customers.get(randCustomerIndex) + " to: " + randFuture);
            CommGenerator.counter++;
            DDB.putItem(customers.get(randCustomerIndex), randFuture, true);
            try {
                Thread.sleep((long) (Math.random() * 10000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
