package surfstore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
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
    private final int BLOCK_SIZE = 4096;

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

    private List<byte []> fileToBlocks(byte[] data){
        int numBlocks = 1 + data.length/4096;
        List<byte []> result = new ArrayList<>();
        for(int i=0; i<numBlocks; i++){
            if(i<numBlocks-1) result.add(Arrays.copyOfRange(data, i*BLOCK_SIZE,  (i+1)*BLOCK_SIZE));
            else result.add(Arrays.copyOfRange(data, i*BLOCK_SIZE,  data.length));
        }

        return result;

    }

    private void callGetVersion(String fileName){
        FileInfo localFile = FileInfo.newBuilder().setFilename(fileName).build();
        FileInfo remoteFile = metadataStub.getVersion(localFile);
        System.out.println(remoteFile.getVersion()+"");
    }

    private void callUpload(String filePath){
        File localFile = new File(filePath);
        String fileName = localFile.getName();

        FileInfo.Builder localFileBuilder = FileInfo.newBuilder().setFilename(fileName);
        FileInfo remoteFile = metadataStub.getVersion(localFileBuilder.build());
        localFileBuilder.setVersion(remoteFile.getVersion() + 1);
        try{
            byte[] data = Files.readAllBytes(Paths.get(filePath));
            Map<String, byte[]> existingBlocks = addHashes(localFileBuilder, data);

            WriteResult modifyResponse = metadataStub.modifyFile(localFileBuilder.build());

            //Retry upload
            while(modifyResponse.getResultValue() != WriteResult.Result.OK_VALUE) {
                if (modifyResponse.getResultValue() == WriteResult.Result.MISSING_BLOCKS_VALUE) {
                    for (String missingHash : modifyResponse.getMissingBlocksList()) {
                        Block b = Block.newBuilder().setHash(missingHash)
                                .setData(ByteString.copyFrom(existingBlocks.get(missingHash)))
                                .build();

                        blockStub.storeBlock(b);
                        logger.info("upload block: " + missingHash);
                    }
                }

                int newVersion = modifyResponse.getCurrentVersion() + 1;
                FileInfo.Builder builder = FileInfo.newBuilder().setFilename(fileName);

                builder.setVersion(newVersion);
                addHashes(builder, data);

                modifyResponse = metadataStub.modifyFile(builder.build());
            }

            System.out.println("OK");

        } catch (IOException e){
            logger.severe("Caught IOException: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }

    private Map<String, byte[]> addHashes(FileInfo.Builder localFileBuilder, byte[] data) {
        List<byte[]> blockList = fileToBlocks(data);
        int numBlocks = blockList.size();
        Map<String, byte[]> blocks = new HashMap<>();
        for(int i=0; i<numBlocks; i++){
            String hash;
            hash = toSHA256(blockList.get(i));
            localFileBuilder.addBlocklist(hash);
            blocks.put(hash, blockList.get(i));
        }
        return blocks;
    }

    private void callDownload(String fileName, String directory){
        FileInfo localFile = FileInfo.newBuilder().setFilename(fileName).build();
        FileInfo remoteFile = metadataStub.readFile(localFile);

        if(remoteFile.getVersion() == 0 || (remoteFile.getBlocklistCount() == 1 && remoteFile.getBlocklist(0).equals("0")) ) {
            System.out.println("Not Found");
        }
        else{
            try{
                if(directory.equals("")) directory = System.getProperty("user.dir");
                Map<String, byte[]> existingBlocks = new HashMap<>();

                //scan current directory for all blocks
                File folder = new File(directory);
                for(final File fileEntry: folder.listFiles()){
                    if(fileEntry.isFile()){
                        List<byte[]> blocks = fileToBlocks(Files.readAllBytes(fileEntry.toPath()));
                        for(byte[] block: blocks){
                            String hash = toSHA256(block);
                            existingBlocks.put(hash, block);
                        }
                    }
                }

                FileOutputStream fos = new FileOutputStream(directory+"/"+fileName);
                for(String hash: remoteFile.getBlocklistList()){
                    byte[] data = existingBlocks.get(hash);
                    if(data == null){
                        Block b = Block.newBuilder().setHash(hash).build();
                        ensure(blockStub.hasBlock(b).getAnswer());
                        data = blockStub.getBlock(b).getData().toByteArray();
                        logger.info("getting block from blockStore");
                    }
                    fos.write(data);
                    logger.info("writing block: " + hash);
                }
                System.out.println("OK");
            } catch (Exception e){
                logger.severe("Caught Exception: " + e.getMessage());
                e.printStackTrace();
                System.exit(2);
            }
        }
    }

    private void callDelete(String filePath){
        File localFile = new File(filePath);
        String fileName = localFile.getName();

        FileInfo.Builder localFileBuilder = FileInfo.newBuilder().setFilename(fileName);
        FileInfo remoteFile = metadataStub.getVersion(localFileBuilder.build());
        if(remoteFile.getVersion() == 0) {
            System.out.println("Not Found");
            return;
        }

        localFileBuilder.setVersion(remoteFile.getVersion() + 1);
        WriteResult deleteResult = metadataStub.deleteFile(localFileBuilder.build());
        while(deleteResult.getResult() != WriteResult.Result.OK) {
            FileInfo.Builder builder = FileInfo.newBuilder().setFilename(fileName).setVersion(deleteResult.getCurrentVersion() + 1);
            deleteResult = metadataStub.deleteFile(builder.build());
        }
        ensure(deleteResult.getResultValue() == 0);
        System.out.println("OK");
    }

    private void go(String method, String fileName, String directory){
        metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");

        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        if(method.equals("getversion")){
            callGetVersion(fileName);
        }else if(method.equals("upload")){
            callUpload(fileName);
        }else if(method.equals("download")){
            callDownload(fileName, directory);
        }else if(method.equals("delete")){
            callDelete(fileName);
        }else{
            logger.severe("The requested operation is not supported. Try upload, download, delete or getversion.");
        }

    }

    /*
     * TODO: Add command line handling here
     */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build().description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class).help("Path to configuration file");
        parser.addArgument("method").type(String.class).help("upload, download, delete or getversion");
        parser.addArgument("path_to_file").type(String.class).help("Path to the file (in case of local) or remote file name");
        parser.addArgument("directory").nargs("?").setDefault("").type(String.class).help("Directory to store downloaded file");

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
        String directory = c_args.getString("directory");

        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);

        try {
            client.go(method.toLowerCase(), fileName, directory);
        } finally {
            client.shutdown();
        }
    }

}
