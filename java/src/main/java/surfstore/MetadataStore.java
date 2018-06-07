package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

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
    
	protected void start(int port, int numThreads, int nodeNumber) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(this.blockStub, this.config, 1))
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

        Integer nodeNumber = c_args.getInt("number");
		if (nodeNumber > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", nodeNumber));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(nodeNumber), c_args.getInt("threads"), 1);
        server.blockUntilShutdown();
    }

    static class NodeInfo{
    		private int port;
    		private boolean isLeader;
    		
    		public NodeInfo(int port, boolean isLeader) {
    			this.port = port;
    			this.isLeader = isLeader;
    		}
    }
    
    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
    		private Map<String, FileInfo> fileInfos;
    		private List<FileInfo> logList;
    		private int lastCommitted;
    		private BlockStoreBlockingStub blockStoreStub;
    		private boolean isLeader;
    		private List<NodeInfo> nodeList;
    		private int leaderPort;
    		private boolean isUp;

	    	public MetadataStoreImpl(BlockStoreBlockingStub blockStub, ConfigReader config, int nodeNumber) {
	    		super();
	    		this.blockStoreStub = blockStub;
	    		
	    		int leaderNum = config.getLeaderNum();
			this.isLeader = nodeNumber == leaderNum;
	    		this.leaderPort = config.getMetadataPort(leaderNum);
	    		
	    		this.lastCommitted = -1;
	    		this.logList = new ArrayList<FileInfo>();
	    		this.isUp = true;
	    		
	    		this.nodeList = new ArrayList<NodeInfo>();
			nodeList.add(new NodeInfo(this.leaderPort, true));

	    		for(int i=1; i<=config.getNumMetadataServers(); i++) {
	    			//add all other nodes that aren't leader
	    			if(i != nodeNumber && i != leaderNum) {	    				
	    				nodeList.add(new NodeInfo(config.getMetadataPort(i), false));
	    			}
	    		}
	    		
	    		this.fileInfos = new HashMap<String, FileInfo>();
		}

			@Override
	    	public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
	    		Empty response = Empty.newBuilder().build();
	    		responseObserver.onNext(response);
	    		responseObserver.onCompleted();
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
        			responseFile = FileInfo.newBuilder().setFilename("").setFilename(requestedFilename).setVersion(0).build();
        		}

        		responseObserver.onNext(responseFile);
        		responseObserver.onCompleted();
		}

		@Override
		public void modifyFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
			int newVersion = request.getVersion();
			
			ProtocolStringList hashList = request.getBlocklistList();
			
			boolean hasNewBlock = false;
			WriteResult.Builder builder = WriteResult.newBuilder();
			
			int currentVersion = getCurrentVersion(request);
			
			if(!this.isLeader)
			{
				builder.setResult(WriteResult.Result.NOT_LEADER);
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				return;
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
					logList.add(request);
					//TODO: send 2pc log append vote request
					//TODO: wait ... ms, if majority voted, send commit to every followers
					commitLog();
				}
			}

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		@Override
		public void deleteFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
			int newVersion = request.getVersion();

			WriteResult.Builder builder = WriteResult.newBuilder();
			
			if(!this.isLeader)
			{
				builder.setResult(WriteResult.Result.NOT_LEADER);
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				return;
			}
			
			int currentVersion = getCurrentVersion(request);
			
			if(newVersion != currentVersion + 1) {
				builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(currentVersion);
			}
			else {
				builder.setResult(WriteResult.Result.OK);
				builder.setCurrentVersion(newVersion);
				
				FileInfo log = FileInfo.newBuilder().setFilename(request.getFilename()).setVersion(newVersion).addBlocklist("0").build();
				logList.add(log);
				commitLog();
			}

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		private int getCurrentVersion(FileInfo request) {
			String requestedFilename = request.getFilename();
			int currentVersion = 0;
			if(fileInfos.containsKey(requestedFilename)) {
				FileInfo existingFileInfo = fileInfos.get(requestedFilename);
				currentVersion = existingFileInfo.getVersion();			
			}
			return currentVersion;
		}

		private void commitLog() {
			lastCommitted += 1;
			FileInfo log = logList.get(lastCommitted);
			fileInfos.put(log.getFilename(), log);
		}
		
		private void printFileSummary() {
			System.out.println("File Summary");
			
			for(FileInfo f:this.fileInfos.values()) {
				System.out.println(f.getFilename() + " " + f.getVersion());
			}
			
			System.out.println("------------");
		}

		@Override
		public void isLeader(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
			responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(this.isLeader).build());
			responseObserver.onCompleted();
		}

		@Override
		public void crash(Empty request, StreamObserver<Empty> responseObserver) {
			// TODO Auto-generated method stub
			this.isUp = false;
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		}

		@Override
		public void restore(Empty request, StreamObserver<Empty> responseObserver) {
			// TODO Auto-generated method stub
			this.isUp = true;
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		}

		@Override
		public void isCrashed(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
			// TODO Auto-generated method stub
			responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(!this.isUp).build());
			responseObserver.onCompleted();
		}

		@Override
		public void getVersion(FileInfo request, StreamObserver<FileInfo> responseObserver) {
			// TODO Auto-generated method stub
			FileInfo ans;
			if(this.getCurrentVersion(request) != 0) {
				ans = fileInfos.get(request.getFilename());
			}
			else
			{
				ans = FileInfo.newBuilder().setFilename(request.getFilename()).setVersion(0).build();
			}
			responseObserver.onNext(ans);
			responseObserver.onCompleted();
		}
    }
}