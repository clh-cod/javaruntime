package io.grpc.examples.javaruntime;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.io.*;
import java.net.URLClassLoader; 
import java.lang.reflect.Method;
import java.net.URL;
import org.yaml.snakeyaml.Yaml;
import com.google.protobuf.ByteString;
import java.nio.*;
import java.nio.charset.*;
import java.net.InetSocketAddress;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JavaRuntimeServer {
    private static final Logger logger = Logger.getLogger(JavaRuntimeServer.class.getName());

    private static final Map<String,String> config = new TreeMap<String,String>();

    private String host;
    private int port;
    
    private Server server;

    public JavaRuntimeServer() {
        config.put("name","baetyl-java");
        config.put("confPath","/etc/baetyl/service.yml");
        config.put("codePath","/var/lib/baetyl/code");
        config.put("serverAddress","0.0.0.0:50051");
    }

    public void Init() {
        Map<String, String> systemEnv = System.getenv();
        if(systemEnv.containsKey("BAETYL_SERVICE_NAME")) {
            config.replace("name",systemEnv.get("BAETYL_SERVICE_NAME"));
        }

        if(systemEnv.containsKey("BAETYL_CONF_FILE")) {
            config.replace("confPath",systemEnv.get("BAETYL_CONF_FILE"));
        }

        if(systemEnv.containsKey("BAETYL_CODE_PATH")) {
            config.replace("codePath",systemEnv.get("BAETYL_CODE_PATH"));
        }

        if(systemEnv.containsKey("BAETYL_SERVICE_ADDRESS")) {
            config.replace("serverAddress",systemEnv.get("BAETYL_SERVICE_ADDRESS"));
        }

        String serverAddress = config.get("serverAddress");
        String[] addressArray = serverAddress.split(":");
        host = addressArray[0];

        try {
            port = Integer.parseInt(addressArray[1]);            
            server = NettyServerBuilder.forAddress(new InetSocketAddress(host, port))
                .addService(new FunctionImpl(logger,config.get("confPath"),config.get("codePath")))
                .maxInboundMessageSize(4 * 1024 * 1024)
                .build(); 
            
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }               
    }

    public void Start() throws IOException {
        server.start();
        logger.info("service starting, listening at " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    JavaRuntimeServer.this.stop();
                } catch (InterruptedException e){
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final JavaRuntimeServer server = new JavaRuntimeServer();
        server.Init();
        server.Start();
        server.blockUntilShutdown();
    }

    static class FunctionImpl extends FunctionGrpc.FunctionImplBase {        
        private static final Map<String,Object> functionHandler = new TreeMap<String,Object>();
        Logger logger;
        String confPath;
        String codePath;

        public FunctionImpl(Logger logger,String confPath,String codePath) {
            this.logger = logger;
            this.confPath = confPath;
            this.codePath = codePath;
        }

        public void call(Message req, StreamObserver<Message> responseObserver) {            
            try {
                Message reply = Message.newBuilder().setPayload(getFunctionPayload(req))
                                .setID(req.getID()).putAllMetadata(req.getMetadataMap()).build(); 
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch(Exception e) {
                logger.info("Error:" + e.toString());
            }
        }

        public boolean getFunction() {            
            try {      
                Yaml yaml = new Yaml();
                Map functionMap = yaml.loadAs(new FileInputStream(confPath),Map.class);
                if(!functionMap.containsKey("functions")) {
                    //不存在functions,文件错误
                    return false;
                }
                ArrayList obj = (ArrayList)functionMap.get("functions");
                for(int i = 0; i < obj.size(); i++) {
                    LinkedHashMap linkMap = (LinkedHashMap)obj.get(i);

                    String name = linkMap.get("name").toString();
                    String handler = linkMap.get("handler").toString();
                    String codedir = linkMap.get("codedir").toString();

                    String[] moduleHandler = handler.split("\\.");
                    String Path;
                    if(codedir.equals(".")) {
                        Path = codePath + "/" + moduleHandler[0] + ".jar";//.jar包名、类名
                    } else {
                        Path = codePath + "/" + codedir + moduleHandler[0] + ".jar";//.jar包名、类名
                    }

                    Map<String,String> functionModule = new TreeMap<String,String>();
                    functionModule.put("className",moduleHandler[0]);
                    functionModule.put("Method",moduleHandler[1]);
                    functionModule.put("classPath",Path);
                    functionHandler.put(name,functionModule);
                }
            } catch (Exception e) {
                logger.info("Error: " + e.toString());
            } 
            return true;
        }

        /**
         * 调用.jar包中的某个方法，返回payload
         */
        private ByteString getFunctionPayload(Message req) throws Exception {
            if(!getFunction()) {
                logger.info("Error: this function is not exited!");
                throw new RuntimeException("this function is not exited!");
            }

            String functionName = req.getMetadataMap().get("functionName");
            if(functionName == null) {
                if(functionHandler.size() < 1) {
                    logger.info("no functions exist");
                    throw new RuntimeException("no functions exist");
                }
                functionName = getKeyOrNull(functionHandler);
            }

            if(!functionHandler.containsKey(functionName)) {
                logger.info("the function doesn't found: " + functionName);
                throw new RuntimeException("the function doesn't found: " + functionName);
            }

            Map<String,String> functionModule = (Map<String,String>)functionHandler.get(functionName);
            String softPath = "file:" + functionModule.get("classPath");
            URLClassLoader classLoader = new URLClassLoader(new URL[]{new URL(softPath)},Thread.currentThread().getContextClassLoader());
            //类名Demo
            Class demo = classLoader.loadClass(functionModule.get("className"));
            /**
             * 方法名invoke
             * String invoke(String name) {}
             */
		    Method method = demo.getMethod(functionModule.get("Method"),String.class); 
            Object object = demo.getDeclaredConstructor().newInstance();

            String payloadString = ByteString_toString(req.getPayload());
            String payload = method.invoke(object,payloadString).toString();     
            
            if(payload == null ) {
                return null;
            }
            return toByteStringUtf8(payload);
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
        
        private String getKeyOrNull(Map<String, Object> map) {
            String obj = null;
            for (Entry<String, Object> entry : map.entrySet()) {
                obj = entry.getKey();
                // obj = entry.getValue();
                if (obj != null) {
                    break;
                }
            }
            return  obj;
        }
    }
}