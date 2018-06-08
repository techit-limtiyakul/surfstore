package surfstore;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import surfstore.BlockStoreGrpc.BlockStoreBlockingStub;
import surfstore.MetadataStoreGrpc.MetadataStoreBlockingStub;
import surfstore.SurfStoreBasic.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;


class MetaDataStoreTwoPhaseTest {
    private static BlockStore blockStoreServer;
    private static MetadataStore leader;
    private static MetadataStore f1;
    private static MetadataStore f2;

    private static BlockStoreBlockingStub blockStoreStub;
    private static MetadataStoreBlockingStub leaderStub;
    private static MetadataStoreBlockingStub f1Stub;
    private static MetadataStoreBlockingStub f2Stub;
    private static ConfigReader config;

    @BeforeAll
    static void readConf() throws IOException {
        File configf = new File("src/test/resources/configDistributed.txt");
        config = new ConfigReader(configf);
        blockStoreServer = new BlockStore(config);
        leader = new MetadataStore(config);
        f1 = new MetadataStore(config);
        f2 = new MetadataStore(config);
    }

    @BeforeEach
    void setUp() throws IOException {
        blockStoreStub = createBlockStoreStub(config);
        leaderStub = createMetadataStoreStub(leader, config.getLeaderNum(), config.getMetadataPort(config.getLeaderNum()));
        f1Stub = createMetadataStoreStub(f1, 2, config.getMetadataPort(2));
        f2Stub = createMetadataStoreStub(f2,3, config.getMetadataPort(3));
    }

    @AfterEach
    void teardown() throws IOException{
        blockStoreServer.stop();
        leader.stop();
        f1.stop();
        f2.stop();
    }

    @Test
    void sanityCheck() throws InterruptedException{

        FileInfo localFile = createNewFile("file1");

        FileInfo fileInfo  = leaderStub.readFile(localFile);
        assertEquals(fileInfo.getVersion(), 0);

        WriteResult file1Write = leaderStub.modifyFile(localFile);
        assertEquals(file1Write.getResultValue(), WriteResult.Result.OK_VALUE);

        assertTrue(inSyncWithLeader(localFile , f1Stub));
        assertTrue(inSyncWithLeader(localFile, f2Stub));

    }

    @Test
    void crashBeforeCommit() throws InterruptedException{

        FileInfo localFile = createNewFile("file2");

        FileInfo fileInfo  = leaderStub.readFile(localFile);
        assertEquals(fileInfo.getVersion(), 0);

        f1Stub.crash(Empty.newBuilder().build());

        WriteResult file1Write = leaderStub.modifyFile(localFile);
        assertEquals(file1Write.getResultValue(), WriteResult.Result.OK_VALUE);

        Thread.sleep(500);

        assertThrows(AssertionError.class, () -> inSyncWithLeader(localFile, f1Stub));
        assertTrue(inSyncWithLeader(localFile, f2Stub));
    }

    @Test
    void restoreAfterCommit() throws InterruptedException{
        FileInfo localFile = createNewFile("file3");

        FileInfo fileInfo  = leaderStub.readFile(localFile);
        assertEquals(fileInfo.getVersion(), 0);

        f2Stub.crash(Empty.newBuilder().build());

        WriteResult file1Write = leaderStub.modifyFile(localFile);
        assertEquals(file1Write.getResultValue(), WriteResult.Result.OK_VALUE);

        assertTrue(f2Stub.isCrashed(Empty.newBuilder().build()).getAnswer());
        assertFalse(f1Stub.isCrashed(Empty.newBuilder().build()).getAnswer());

        assertTrue(inSyncWithLeader(localFile, f1Stub));
        assertThrows(AssertionError.class, () -> inSyncWithLeader(localFile, f2Stub));

        f2Stub.restore(Empty.newBuilder().build());

        FileInfo updatedFile = createNewFile("file3", 2);

        assertEquals(leaderStub.deleteFile(updatedFile).getResultValue(), WriteResult.Result.OK_VALUE);

        assertTrue(inSyncWithLeader(updatedFile, f1Stub));
        assertTrue(inSyncWithLeader(updatedFile, f2Stub));

    }

    @Test
    void callFollowerDuringCrash(){
        FileInfo localFile = createNewFile("file4");

        FileInfo fileInfo  = leaderStub.readFile(localFile);
        assertEquals(fileInfo.getVersion(), 0);

        f2Stub.crash(Empty.newBuilder().build());

        WriteResult file1Write = leaderStub.modifyFile(localFile);
        assertEquals(file1Write.getResultValue(), WriteResult.Result.OK_VALUE);

        assertEquals(f2Stub.modifyFile(fileInfo).getResultValue(), WriteResult.Result.NOT_LEADER_VALUE);
        assertEquals(f2Stub.deleteFile(fileInfo).getResultValue(), WriteResult.Result.NOT_LEADER_VALUE);
        assertEquals(f2Stub.readFile(fileInfo).getFilename(), fileInfo.getFilename());
        assertEquals(f2Stub.getVersion(fileInfo).getVersion(), fileInfo.getVersion());
    }


    @Test
    void eventSeries(){

    }

    private FileInfo createNewFile(String fileName){
        return createNewFile(fileName, 1);
    }

    private FileInfo createNewFile(String fileName, int version){

        Block b1 = Block.newBuilder().setHash("firstBlock").setData(ByteString.copyFrom("abc", Charset.forName("UTF-8"))).build();
        Block b2 = Block.newBuilder().setHash("secondBlock").setData(ByteString.copyFrom("xyz", Charset.forName("UTF-8"))).build();
        if(!blockStoreStub.hasBlock(b1).getAnswer()) blockStoreStub.storeBlock(b1);
        if(!blockStoreStub.hasBlock(b2).getAnswer()) blockStoreStub.storeBlock(b2);

        return FileInfo.newBuilder()
                .setFilename(fileName)
                .setVersion(version)
                .addBlocklist("firstBlock")
                .addBlocklist("secondBlock")
                .build();
    }

    private boolean inSyncWithLeader(FileInfo fileInfo,  MetadataStoreBlockingStub follower){

        FileInfo leaderResponse = leaderStub.readFile(fileInfo);
        FileInfo fResponse = follower.readFile(fileInfo);

        boolean versionSync = leaderResponse.getVersion() == fResponse.getVersion();
        System.out.println(leaderResponse.getBlocklistList() + " " +fResponse.getBlocklistList());
        assertThat(leaderResponse.getBlocklistList(), is(fResponse.getBlocklistList()));

        return versionSync;

    }

    private static BlockStoreBlockingStub createBlockStoreStub(ConfigReader config) throws IOException {
        blockStoreServer.start(config.getBlockPort(), 1);

        ManagedChannel blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        return BlockStoreGrpc.newBlockingStub(blockChannel);
    }

    private static MetadataStoreBlockingStub createMetadataStoreStub(MetadataStore server, int serverId, int port) throws IOException {
        server.start(port, 1, serverId);

        ManagedChannel mdChannel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext(true).build();

        return MetadataStoreGrpc.newBlockingStub(mdChannel);
    }

}
