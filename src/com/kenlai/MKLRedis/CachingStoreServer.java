package com.kenlai.MKLRedis;

import java.util.concurrent.TimeUnit;

import com.kenlai.MKLRedis.RequestQueue.AsyncTask;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * Netty server template code derived from netty.io user guide.
 */
public class CachingStoreServer {
    private int initialSize = Integer.getInteger("initialSize", 1024);

    private static final StringDecoder DECODER = new StringDecoder();
    private static final StringEncoder ENCODER = new StringEncoder();

    private CommandProcessor processor;
    private CachingStore store;
    private RequestQueue queue;

    private int port;

    public CachingStoreServer(int port) {
        this.port = port;
        store = new CachingStore(initialSize);
        processor = new CommandProcessor(store);
        queue = new RequestQueue();
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline pipeline = ch.pipeline();

                     // Add the text line codec combination first,
                     pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                     // the encoder and decoder are static as these are sharable
                     pipeline.addLast(DECODER);
                     pipeline.addLast(ENCODER);
                     pipeline.addLast(new CachingStoreServerHandler(queue));
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 128)
             .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync();
            // ChannelFuture to detect when server socket is closed.
            f = f.channel().closeFuture();

            // Use main thread to process requests
            // TODO: consider separate executor
            while (!f.isDone()) {
                AsyncTask t = queue.poll(1, TimeUnit.SECONDS);
                if (t != null) {
                    String result = processor.process(t.getRequest());
                    t.getCompletableFuture().complete(result);
                }
            }

            // Wait until the server socket is closed.
            f.sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 5555;
        }
        new CachingStoreServer(port).run();
    }

}
