package surfstore;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.xml.ws.Response;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.BlockStoreGrpc.BlockStoreBlockingStub;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.FileInfo.Builder;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

	private ManagedChannel blockChannel;
	private BlockStoreBlockingStub blockStub;
	
    @SuppressWarnings("deprecation")
	public MetadataStore(ConfigReader config) {
    		this.config = config;
		
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

	}

	protected void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(this.blockStub))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    protected void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

	private static void add(BlockStoreBlockingStub blockStoreStub, String value) {
		surfstore.SurfStoreBasic.Block.Builder data = Block.newBuilder().setHash(value).setData(ByteString.copyFrom("ss", Charset.forName("UTF-8")));
		blockStoreStub.storeBlock(data.build());
	}

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
    		private Map<String, FileInfo> fileInfos;
    		private BlockStoreBlockingStub blockStoreStub;

	    	public MetadataStoreImpl(BlockStoreBlockingStub blockStub) {
	    		super();
	    		this.blockStoreStub = blockStub;
	    		this.fileInfos = new HashMap<String, FileInfo>();
		}

			@Override
	    	public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
	    		Empty response = Empty.newBuilder().build();
//	    		responseObserver.onNext(response);
//	    		responseObserver.onCompleted();
	    	}
	    	
        /* (non-Javadoc)
         * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#readFile(surfstore.SurfStoreBasic.FileInfo, io.grpc.stub.StreamObserver)
         */
        /* (non-Javadoc)
         * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#readFile(surfstore.SurfStoreBasic.FileInfo, io.grpc.stub.StreamObserver)
         */
        /* (non-Javadoc)
         * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#readFile(surfstore.SurfStoreBasic.FileInfo, io.grpc.stub.StreamObserver)
         */
        @Override
		public void readFile(FileInfo request, StreamObserver<FileInfo> responseObserver) {
        		String requestedFilename = request.getFilename();
        		
        		FileInfo responseFile;
        		if(fileInfos.containsKey(requestedFilename)) {
        			responseFile = fileInfos.get(requestedFilename);
        		}
        		else
        		{
        			responseFile = FileInfo.newBuilder().setFilename("").setVersion(0).build();

        		}

        		responseObserver.onNext(responseFile);
        		responseObserver.onCompleted();
		}

		@Override
		public void modifyFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
			String requestedFilename = request.getFilename();
			int newVersion = request.getVersion();
			
			ProtocolStringList hashList = request.getBlocklistList();
			
			boolean hasNewBlock = false;
			WriteResult.Builder builder = WriteResult.newBuilder();
			
			int currentVersion = 0;
			if(fileInfos.containsKey(requestedFilename)) {
				FileInfo existingFileInfo = fileInfos.get(requestedFilename);
				currentVersion = existingFileInfo.getVersion();			
			}
			
			if(newVersion != currentVersion + 1) {
				builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(currentVersion);
			}
			else {
				for(String hash: hashList) {
					Block block = Block.newBuilder().setHash(hash).build();
					if(!this.blockStoreStub.hasBlock(block).getAnswer()) {
						builder.addMissingBlocks(hash);
						hasNewBlock = true; 
					}
				}
				
				if(hasNewBlock) {
					// blocks not exist in the block store 
					builder.setResult(WriteResult.Result.MISSING_BLOCKS);
					builder.setCurrentVersion(currentVersion);
				}
				else {
					builder.setResult(WriteResult.Result.OK);
					builder.setCurrentVersion(newVersion);
					// good request, update log, commit
					fileInfos.put(requestedFilename, request);
				}
			}

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		private void printFileSummary() {
			System.out.println("File Summary");

			for(FileInfo f:this.fileInfos.values()) {
				System.out.println(f.getFilename() + " " + f.getVersion());
			}
			
			System.out.println("------------");
		}

		@Override
		public void deleteFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
			String requestedFilename = request.getFilename();
			int newVersion = request.getVersion();

			WriteResult.Builder builder = WriteResult.newBuilder();
			
			int currentVersion = 0;
			if(fileInfos.containsKey(requestedFilename)) {
				FileInfo existingFileInfo = fileInfos.get(requestedFilename);
				currentVersion = existingFileInfo.getVersion();			
			}
			
			if(newVersion != currentVersion + 1) {
				builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(currentVersion);
			}
			else {
				builder.setResult(WriteResult.Result.OK);
				builder.setCurrentVersion(newVersion);
				// good request, update log, commit
				
				fileInfos.put(requestedFilename, FileInfo.newBuilder().setFilename(requestedFilename).setVersion(newVersion).build());
			}

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		@Override
		public void isLeader(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
			// TODO Auto-generated method stub
			super.isLeader(request, responseObserver);
		}

		@Override
		public void crash(Empty request, StreamObserver<Empty> responseObserver) {
			// TODO Auto-generated method stub
			super.crash(request, responseObserver);
		}

		@Override
		public void restore(Empty request, StreamObserver<Empty> responseObserver) {
			// TODO Auto-generated method stub
			super.restore(request, responseObserver);
		}

		@Override
		public void isCrashed(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
			// TODO Auto-generated method stub
			super.isCrashed(request, responseObserver);
		}

		@Override
		public void getVersion(FileInfo request, StreamObserver<FileInfo> responseObserver) {
			// TODO Auto-generated method stub
			super.getVersion(request, responseObserver);
		}
    }
}