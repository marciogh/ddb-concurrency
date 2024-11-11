package com.marciogh;

import java.util.List;
import java.util.Random;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class CommConsumer implements Runnable {

    static List<String> customers;
    public static volatile Integer counter;
    private Random random = new Random();

    public CommConsumer(List<String> customers) {
        CommConsumer.counter = 0;
        CommConsumer.customers = customers;
    }

    @Override
    public void run() {
        while (true) {
            System.out.println("Consumer counter: " + CommConsumer.counter);
            QueryResponse q = DDB.queryPending(customers.get(random.nextInt(customers.size())));
            int counter = DDB.process(q);
            synchronized (CommConsumer.class) {
                CommConsumer.counter += counter;
                if (CommConsumer.counter == 1000) {
                    break;
                }
            }
            try {
                Thread.sleep((long) (Math.random() * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
