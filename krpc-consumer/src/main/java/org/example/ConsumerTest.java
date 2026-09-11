package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.client.proxy.ClientProxy;
import org.example.pojo.User;
import org.example.service.UserService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ConsumerTest {

    private static final int THREAD_POOL_SIZE = 20;
    private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static void main(String[] args) throws InterruptedException{
        // ClientProxy clientProxy = new ClientProxy("127.0.0.1", 9999, 0);
        ClientProxy clientProxy = new ClientProxy();
        UserService proxy = clientProxy.getProxy(UserService.class);

        for (int i = 0; i < 1; i++) {
            Integer i1 = i;
            if (i % 30 == 0) {
                Thread.sleep(3000);
            }

            executorService.submit(() -> {
                try {
                    User user = proxy.getUserByUserId(i1);
                    if (user != null) {
                        log.info("从服务端得到的user={}", user);
                    } else {
                        log.warn("获取的user为null，userId={}", i1);
                    }
                    Integer id = proxy.insertUserId(User.builder().id(i1).username("User" + i1).gender(true).build());
                    if (id != null) {
                        log.info("插入userId={}", id);
                    } else {
                        log.warn("插入失败，userId={}", id);
                    }
                } catch (Exception e) {
                    log.error("调用服务异常，userId={}", i1, e);
                }
            });
        }
        executorService.shutdown();
    }
}
