package org.example.service;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

class ExclusiveVsSharedDemo {
    public static void main(String[] args) {
        // 独占模式：同一时刻只有 1 个线程能进入临界区
        ReentrantLock lock = new ReentrantLock();

        // 共享模式：同一时刻最多 3 个线程能进入临界区
        Semaphore semaphore = new Semaphore(3);

        // 独占模式示例
        Runnable exclusiveTask = () -> {
            lock.lock();
            try {
                System.out.println(Thread.currentThread().getName()
                        + " 获取到独占锁，正在执行...");
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        };
        Thread.interrupted();
        // 共享模式示例
        Runnable sharedTask = () -> {
            try {
                semaphore.acquire();
                System.out.println(Thread.currentThread().getName()
                        + " 获取到许可证，正在执行...");
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                semaphore.release();
            }
        };

        System.out.println("=== 独占模式（ReentrantLock）===");
        for (int i = 0; i < 5; i++) {
            new Thread(exclusiveTask, "独占线程-" + i).start();
        }

        try { Thread.sleep(3000); } catch (InterruptedException e) { }

        System.out.println("\n=== 共享模式（Semaphore）===");
        for (int i = 0; i < 5; i++) {
            new Thread(sharedTask, "共享线程-" + i).start();
        }
    }
}
