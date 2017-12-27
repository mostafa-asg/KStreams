package com.github;

public class Driver {

    public static void main(String[] args) {

        // Start a http server on localhost:4567
        final HttpServer httpServer = new HttpServer();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                httpServer.stop();
            }
        });

        httpServer.start();
    }

}
