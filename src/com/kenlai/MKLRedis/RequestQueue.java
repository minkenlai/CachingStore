package com.kenlai.MKLRedis;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RequestQueue {
    private LinkedBlockingQueue<AsyncTask> taskQueue =
            new LinkedBlockingQueue<AsyncTask>();

    /**
     * Enqueue request to be executed by single thread. Returns immediately.
     *
     * @param request full command string
     * @return CompletableFuture to get result/status of the request
     */
    public CompletableFuture<String> add(String request) {
        CompletableFuture<String> cf = new CompletableFuture<String>();
        taskQueue.add(new AsyncTask(request, cf));
        return cf;
    }

    public AsyncTask poll(long timeout, TimeUnit timeUnit)
            throws InterruptedException {
        return taskQueue.poll(timeout, timeUnit);
    }

    public static class AsyncTask {
        String request;
        CompletableFuture<String> future;
        public AsyncTask(String request, CompletableFuture<String> future) {
            this.request = request;
            this.future = future;
        }
        public String getRequest() {
            return request;
        }
        public CompletableFuture<String> getCompletableFuture() {
            return future;
        }
    }
}
