package surfstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import surfstore.BlockStoreGrpc.BlockStoreBlockingStub;
import surfstore.MetadataStoreGrpc.MetadataStoreBlockingStub;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;

class MetadataStoreIntegrationTest {
	private static BlockStore blockStoreServer;
	private static MetadataStore MDS;
	
	private static BlockStoreBlockingStub blockStoreStub;
	private static MetadataStoreBlockingStub MDSStub;
	
	private static ConfigReader config;
	
	@BeforeAll
	public static void readConf() throws IOException {
        File configf = new File("src/test/java/surfstore/configCentralized.txt");
        config = new ConfigReader(configf);
		blockStoreServer = new BlockStore(config);
		MDS = new MetadataStore(config);
		
		blockStoreStub = createBlockStoreStub(config);
		MDSStub = createMetadataStoreStub(config);
    }
	
	
	//create a new blockstore for fresh data
	@BeforeEach
	public void setup() throws IOException {		
		addToBlockStore("blk1");
		addToBlockStore("blk2");
		addToBlockStore("blk3");
	}
	
	@AfterEach
	public void teardown() {
		try {
			blockStoreServer.stop();
			blockStoreStub = createBlockStoreStub(config);
			
			MDS.stop();
			MDSStub = createMetadataStoreStub(config);
		}
		catch(Exception e) {
			
		}
	}

	public FileInfo createFileInfo(String filename, int version, String... hashes) {
		FileInfo.Builder builder  = FileInfo.newBuilder();
		if(filename != null) {
			builder.setFilename(filename);
		}
		builder.setVersion(version);
		for(String hash: hashes) {
			builder.addBlocklist(hash);
		}
		
		return builder.build();
	}
	
	@Test
	void testAddNewFile() throws Exception {
		FileInfo request = createFileInfo("Hello.txt", 1, "newblk", "blk1", "blk2");
		WriteResult result = MDSStub.modifyFile(request);
		
		assertEquals(WriteResult.Result.MISSING_BLOCKS_VALUE, result.getResultValue());
		
		assertEquals(1, result.getMissingBlocksCount());
		assertTrue(result.getMissingBlocksList().contains("newblk"));
		assertFalse(result.getMissingBlocksList().contains("blk1"));
		
		assertEquals(0, result.getCurrentVersion());
		
		//add to block so newblk exists
		addToBlockStore("newblk");
		WriteResult result2 = MDSStub.modifyFile(request);
		
		assertEquals(WriteResult.Result.OK_VALUE, result2.getResultValue());
		
		assertEquals(0, result2.getMissingBlocksCount());
		assertFalse(result2.getMissingBlocksList().contains("newblk"));
		
		assertEquals(1, result2.getCurrentVersion());
	}

	@Test
	void testModifyFile() throws Exception {
		//assume creating file works properly
		FileInfo request = createFileInfo("Hello.txt", 1, "blk1", "blk2");
		FileInfo request2 = createFileInfo("Hello.txt", 2, "blk1", "blk3");
		MDSStub.modifyFile(request);
		MDSStub.modifyFile(request2);
		
		FileInfo request3 = createFileInfo("Hello.txt", 3, "blk1", "newblk");
		WriteResult result = MDSStub.modifyFile(request3);
		
		assertEquals(WriteResult.Result.MISSING_BLOCKS_VALUE, result.getResultValue());

		assertEquals(1, result.getMissingBlocksCount());
		assertTrue(result.getMissingBlocksList().contains("newblk"));
		assertFalse(result.getMissingBlocksList().contains("blk1"));
		
		assertEquals(2, result.getCurrentVersion());
		
		//add to block so newblk exists
		addToBlockStore("newblk");
		WriteResult result2 = MDSStub.modifyFile(request3);
		
		assertEquals(WriteResult.Result.OK_VALUE, result2.getResultValue());
		
		assertEquals(0, result2.getMissingBlocksCount());
		assertFalse(result2.getMissingBlocksList().contains("newblk"));
		
		assertEquals(3, result2.getCurrentVersion());
	}
	
	
	@Test
	void testBadVersion() throws Exception {
		FileInfo request = createFileInfo("Hello.txt", 0, "blk1", "blk2");
		WriteResult result = MDSStub.modifyFile(request);
		
		assertEquals(WriteResult.Result.OLD_VERSION_VALUE, result.getResultValue());
		assertEquals(0, result.getCurrentVersion());
		
		FileInfo request2 = createFileInfo("Hello.txt", -1, "blk1", "blk2");
		WriteResult result2 = MDSStub.modifyFile(request2);
		
		assertEquals(WriteResult.Result.OLD_VERSION_VALUE, result2.getResultValue());
		assertEquals(0, result2.getCurrentVersion());	
		
		FileInfo request3 = createFileInfo("Hello.txt", 3, "blk1", "blk2");
		WriteResult result3 = MDSStub.modifyFile(request3);
		
		assertEquals(WriteResult.Result.OLD_VERSION_VALUE, result3.getResultValue());
		assertEquals(0, result3.getCurrentVersion());
	}
	
	@Test
	void testBadVersionForExistingFile() throws Exception {
		//assume creating file works properly
		FileInfo request = createFileInfo("Hello.txt", 1, "blk1", "blk2");
		FileInfo request2 = createFileInfo("Hello.txt", 2, "blk1", "blk3");
		MDSStub.modifyFile(request);
		MDSStub.modifyFile(request2);
		
		FileInfo request3 = createFileInfo("Hello.txt", 5, "blk1", "blk3");
		WriteResult result = MDSStub.modifyFile(request3);
		
		
		assertEquals(WriteResult.Result.OLD_VERSION_VALUE, result.getResultValue());
		assertEquals(2, result.getCurrentVersion());
		
		FileInfo request4 = createFileInfo("Hello.txt", 2, "blk1", "blk3");
		WriteResult result2 = MDSStub.modifyFile(request4);
		
		
		assertEquals(WriteResult.Result.OLD_VERSION_VALUE, result2.getResultValue());
		assertEquals(2, result.getCurrentVersion());
	}
	
	
	@Test
	void testReadFile() throws Exception {
		//Not found read
		FileInfo readRequest = createFileInfo("Hello.txt",0);
		FileInfo result = MDSStub.readFile(readRequest);
		
		assertEquals(0, result.getBlocklistCount());
		assertEquals(0, result.getVersion());
		
		//Create
		//assume creating file works properly
		FileInfo request = createFileInfo("Hello.txt", 1, "blk1", "blk2");
		FileInfo request2 = createFileInfo("Hello.txt", 2, "blk1", "blk3");
		MDSStub.modifyFile(request);
		MDSStub.modifyFile(request2);
 
		FileInfo request3 = createFileInfo("File2.txt", 1, "blk2", "blk3");
		MDSStub.modifyFile(request3);

		FileInfo readRequest2 = createFileInfo("Hello.txt",0);
		FileInfo result2 = MDSStub.readFile(readRequest2);
		
		assertEquals(2, result2.getBlocklistCount());
		assertEquals(2, result2.getVersion());
		
		//Edit
		FileInfo request4 = createFileInfo("File2.txt", 2, "blk3");
		MDSStub.modifyFile(request4);
		
		FileInfo readRequest3 = createFileInfo("File2.txt",0);
		FileInfo result3 = MDSStub.readFile(readRequest3);
		
		assertEquals(1, result3.getBlocklistCount());
		assertEquals(2, result3.getVersion());
		
		//Delete
		FileInfo request5 = createFileInfo("Hello.txt", 3);
		MDSStub.deleteFile(request5);
		
		FileInfo result4 = MDSStub.readFile(readRequest2);
		FileInfo result5 = MDSStub.readFile(readRequest3);
		
		assertEquals(1, result4.getBlocklistCount());
		assertTrue(result4.getBlocklistList().contains("0"));
		assertEquals(3, result4.getVersion());
		
		assertEquals(1, result5.getBlocklistCount());
		assertEquals(2, result5.getVersion());
	}
	
	@Test
	void testDelete() throws Exception {
		//Create
		//assume creating file works properly
		FileInfo request = createFileInfo("Hello.txt", 1, "blk1", "blk2");
		FileInfo request2 = createFileInfo("Hello.txt", 2, "blk1", "blk3");
		MDSStub.modifyFile(request);
		MDSStub.modifyFile(request2);
		
		//Bad version
		FileInfo deleteRequest  = createFileInfo("Hello.txt", 2);
		WriteResult result = MDSStub.deleteFile(deleteRequest);
		
		assertEquals(WriteResult.Result.OLD_VERSION_VALUE, result.getResultValue());
		assertEquals(2, result.getCurrentVersion());
		
		//Proper version
		FileInfo deleteRequest2  = createFileInfo("Hello.txt", 3);
		WriteResult result2 = MDSStub.deleteFile(deleteRequest2);
		
		assertEquals(WriteResult.Result.OK_VALUE, result2.getResultValue());
		assertEquals(3, result2.getCurrentVersion());
	}

	private static MetadataStoreBlockingStub createMetadataStoreStub(ConfigReader config) throws IOException {
		MDS.start(config.getMetadataPort(1), 1, 1);
		
		ManagedChannel mdChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
				.usePlaintext(true).build();
		MetadataStoreBlockingStub MDSStub = MetadataStoreGrpc.newBlockingStub(mdChannel);
		
		return MDSStub;
	}
	
	private static BlockStoreBlockingStub createBlockStoreStub(ConfigReader config) throws IOException {
		blockStoreServer.start(config.getBlockPort(), 1);
		
		ManagedChannel blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
				.usePlaintext(true).build();
		BlockStoreBlockingStub blockStoreStub = BlockStoreGrpc.newBlockingStub(blockChannel);
		return blockStoreStub;
	}
	
	private void addToBlockStore(String hash) {
		surfstore.SurfStoreBasic.Block.Builder data = Block.newBuilder().setHash(hash).setData(ByteString.copyFrom("rndm", Charset.forName("UTF-8")));
		blockStoreStub.storeBlock(data.build());
	}

}
