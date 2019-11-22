package com.flume;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.util.ArrayList;
import java.util.List;

public class MyIntercept implements Interceptor {
    public void initialize() {

    }

    /**
     * 拦截source获取的event
     * @param event 接收过滤的event
     * @return event 返回业务处理后的event
     *
     * */
    public Event intercept(Event event) {
        //获取数据body
        byte[] body = event.getBody();
        //将获取的数据转换为大写
        event.setBody(new String(body).toUpperCase().getBytes());
        return event;
    }

    //接收过滤的时间的集合
    public List<Event> intercept(List <Event> list) {
        List<Event> list1 = new ArrayList <Event>();

        for (Event e:list){
            list1.add(intercept(e));
        }
        return list1;
    }

    public void close() {

    }

    public static class Builed implements Interceptor.Builder{

        //获取自定义的拦截器
        public Interceptor build() {
            return new MyIntercept();
        }

        public void configure(Context context) {

        }
    }

}


