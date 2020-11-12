package io.grpc.examples.javaruntime;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.*;
import java.util.*;
import com.google.protobuf.ByteString;

import java.nio.*;
import java.nio.charset.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


public class JavaRuntimeClient {
    private static final Logger logger = Logger.getLogger(JavaRuntimeClient.class.getName());

    private final FunctionGrpc.FunctionBlockingStub blockingStub;

    public JavaRuntimeClient(Channel channel) { 
        blockingStub = FunctionGrpc.newBlockingStub(channel);
    }

    public void greet(long id,ByteString payload,Map<String,String> map) {
        logger.info("Will try to greet " + ByteString_toString(payload) + " ...");
        Message request = Message.newBuilder().setPayload(payload)
                            .setID(id).putAllMetadata(map).build();
        Message response;

        try {
            response = blockingStub.call(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }

        logger.info("ID: " + response.getID());
        logger.info("Payload: " + ByteString_toString(response.getPayload()));
        logger.info("Metadata: " + response.getMetadataMap());
    }

    public ByteString toByteStringUtf8(String str) {
        return ByteString.copyFromUtf8(str);
    }

    public String ByteString_toString(ByteString bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.toByteArray().length);
        Charset charset = StandardCharsets.UTF_8;
        buffer.put(bytes.toByteArray());
        buffer.flip();
        return charset.decode(buffer).toString();
    }

    public static void main(String[] args) throws Exception {
        JSONObject json = new JSONObject();
        json.put("topic","test");
        json.put("payload","hello");        

        // String jsonString = "This is a test message";
        String jsonString = JSON.toJSONString(json);
        ByteString Dir = ByteString.copyFromUtf8(jsonString);

        String target = "localhost:50051";
        Map<String, String> systemEnv = System.getenv();
        if(systemEnv.containsKey("BAETYL_SERVICE_ADDRESS")) {
            target = systemEnv.get("BAETYL_SERVICE_ADDRESS");
        }

        long id = 10;
        Map<String,String> map = new TreeMap<String,String>();
        map.put("messageQOS","1");
        map.put("messageTopic","topic-test");
        // map.put("functionName","test");
        
        ManagedChannel channel = NettyChannelBuilder.forTarget(target)
             .usePlaintext()
             .build();
        
        try {
            JavaRuntimeClient client = new JavaRuntimeClient(channel);
            client.greet(id,Dir,map);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}