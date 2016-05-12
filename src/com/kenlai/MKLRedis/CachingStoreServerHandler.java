package com.kenlai.MKLRedis;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


/**
 * Handles requests. (Derived from netty.io Telnet Server example)
 */
public class CachingStoreServerHandler extends SimpleChannelInboundHandler<String> {
    private static final Long timeout_s = Long.getLong("handlerTimeout", 5L);
    private static final boolean addCrLf = Boolean.getBoolean("addCrLf");

    private RequestQueue queue;

    public CachingStoreServerHandler(RequestQueue queue) {
        this.queue = queue;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) {
        // Generate and write a response.
        String response;
        boolean close = false;
        if ("bye".equals(request.toLowerCase())) {
            response = "Have a good day!\r\n";
            close = true;
        } else {
            try {
                CompletableFuture<String> completableFuture = queue.add(request);
                if (timeout_s == null) {
                    response = completableFuture.get();
                } else {
                    response = completableFuture.get(timeout_s, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                e.printStackTrace();
                response = "ERROR " + e.getLocalizedMessage();
            }
        }
        if (response == null) {
            return;
        }
        if (addCrLf) {
            response = response + "\r\n";
        }

        // We do not need to write a ChannelBuffer here.
        // We know the encoder inserted at TelnetPipelineFactory will do the conversion.
        ChannelFuture future = ctx.write(response);

        // Close the connection after sending 'Have a good day!'
        // if the client has sent 'bye'.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}