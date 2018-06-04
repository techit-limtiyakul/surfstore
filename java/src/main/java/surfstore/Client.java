package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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
import surfstore.SurfStoreBasic.Block.*;

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
        Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }

        builder.setHash(toSHA256(s.getBytes()));
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

	private void go() {
//		metadataStub.ping(Empty.newBuilder().build());
//        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        // TODO: Implement your client here
	    Block b1 = stringToBlock("block_01");
	    Block b2 = stringToBlock("block_02");

	    ensure (!blockStub.hasBlock(b1).getAnswer());
	    ensure(!blockStub.hasBlock(b2).getAnswer());

	    blockStub.storeBlock(b1);
        ensure(blockStub.hasBlock(b1).getAnswer());

        blockStub.storeBlock(b2);
        ensure(blockStub.hasBlock(b2).getAnswer());

        Block b1prime = blockStub.getBlock(b1);
        ensure(b1prime.getHash().equals(b1.getHash()));
        ensure(b1.getData().equals(b1.getData()));

    }

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        
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
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);
        
        try {
        	client.go();
        } finally {
            client.shutdown();
        }
    }

}
