package surfstore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    private static Block stringToBlock(String s){
        Block.Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }

        builder.setHash(toSHA256(s.getBytes(StandardCharsets.UTF_8)));
        return builder.build();
    }
    private void ensure(boolean b) {
        if (!b){
            throw new RuntimeException("Assertion failed!");
        }
    }

    private static String toSHA256(byte[] data){
        MessageDigest digest = null;
        try{
            digest = MessageDigest.getInstance("SHA-256");
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(data);
        return Base64.getEncoder().encodeToString(hash);
    }

    private void callGetVersion(String fileName){
        FileInfo localFile = FileInfo.newBuilder().setFilename(fileName).build();
        FileInfo remoteFile = metadataStub.getVersion(localFile);
        logger.info(remoteFile.getVersion()+"");
    }

    private void callUpload(String fileName){
        FileInfo.Builder localFileBuilder = FileInfo.newBuilder().setFilename(fileName);
        FileInfo remoteFile = metadataStub.getVersion(localFileBuilder.build());

        localFileBuilder.setVersion(remoteFile.getVersion() + 1);


    }

    private void callDownload(String fileName){
        FileInfo localFile = FileInfo.newBuilder().setFilename(fileName).build();
        FileInfo remoteFile = metadataStub.readFile(localFile);

        if(remoteFile.getVersion() == 0 || remoteFile.getBlocklist(0).equals("0") ) logger.info("File not found.");
        else{
            try{
                FileOutputStream fos = new FileOutputStream(fileName);
                for(String hash: remoteFile.getBlocklistList()){
                    Block b = stringToBlock(hash);
                    ensure(blockStub.hasBlock(b).getAnswer());
                    fos.write(blockStub.getBlock(b).getData().toByteArray());
                    logger.info(System.getProperty("user.dir") + fileName);
                }
            } catch (IOException e){
                logger.severe("Caught IOException: " + e.getMessage());
            }
        }
    }

    private void callDelete(String fileName){

    }

	private void go(String method, String fileName){
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");




//        logger.info("Getting file info: "+fileName+", version:" + remoteFile.getVersion());


        if(method.equals("getversion")){
            callGetVersion(fileName);

        }else if(method.equals("upload")){

        }else if(method.equals("download")){
            callDownload(fileName);

        }else if(method.equals("delete")){
            callDelete(fileName);
        }else{
            logger.severe("The requested operation is not supported. Try upload, download, delete or getversion.");
        }

        // TODO: Implement your client here
//	    Block b1 = stringToBlock("block_01");
//	    Block b2 = stringToBlock("block_02");
//
//	    ensure (!blockStub.hasBlock(b1).getAnswer());
//	    ensure(!blockStub.hasBlock(b2).getAnswer());
//
//	    blockStub.storeBlock(b1);
//        ensure(blockStub.hasBlock(b1).getAnswer());
//
//        blockStub.storeBlock(b2);
//        ensure(blockStub.hasBlock(b2).getAnswer());
//
//        Block b1prime = blockStub.getBlock(b1);
//        ensure(b1prime.getHash().equals(b1.getHash()));
//        ensure(b1.getData().equals(b1.getData()));

    }

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build().description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class).help("Path to configuration file");
        parser.addArgument("method").type(String.class).help("upload, download, delete or getversion");
        parser.addArgument("path_to_file").type(String.class).help("Path to the file (in case of local) or remote file name");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }


        File configf = new File(c_args.getString("config_file"));
        String fileName = c_args.getString("path_to_file");
        String method = c_args.getString("method");

        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);
        
        try {
        	client.go(method, fileName);
        } finally {
            client.shutdown();
        }
    }

}
