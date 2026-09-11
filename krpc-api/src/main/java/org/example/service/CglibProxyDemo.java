package org.example.service;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;
import java.util.concurrent.FutureTask;

class OrderService {
    public void createOrder() {
        System.out.println("创建订单");
    }
}

public class CglibProxyDemo {
    public static void main(String[] args) {
        Thread t1 = new Thread(() -> {
            System.out.println(1);
        });
        t1.start();
        t1.run();
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(OrderService.class);

        enhancer.setCallback(new MethodInterceptor() {
            @Override
            public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
                System.out.println("前置增强");
                Object result = proxy.invokeSuper(obj, args);
                System.out.println("后置增强");
                return result;
            }
        });

        OrderService proxy = (OrderService) enhancer.create();
        proxy.createOrder();
    }
}
