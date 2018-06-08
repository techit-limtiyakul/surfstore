package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.logging.Level;
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
import surfstore.MetadataStoreGrpc.MetadataStoreStub;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.CommitMessage;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.LogAnswer;
import surfstore.SurfStoreBasic.LogMessage;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;

public final class MetadataStore {
	private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

	protected Server server;
	protected ConfigReader config;
	MetadataStoreImpl service;
	private ManagedChannel blockChannel;
	private BlockStoreBlockingStub blockStub;

	@SuppressWarnings("deprecation")
	public MetadataStore(ConfigReader config) {
		this.config = config;

		this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true)
				.build();
		this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

	}

	protected void start(int port, int numThreads, int nodeNumber) throws IOException {
		this.service = new MetadataStoreImpl(this.blockStub, this.config, nodeNumber);
		server = ServerBuilder.forPort(port).addService(service)
				.executor(Executors.newFixedThreadPool(numThreads)).build().start();
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
		parser.addArgument("config_file").type(String.class).help("Path to configuration file");
		parser.addArgument("-n", "--number").type(Integer.class).setDefault(1).help("Set which number this server is");
		parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
				.help("Maximum number of concurrent threads");

		Namespace res = null;
		try {
			res = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
		}
		return res;
	}

	public static void main(String[] args) throws Exception {
		Namespace c_args = parseArgs(args);
		if (c_args == null) {
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

	static class NodeInfo {
		private int port;
		private boolean isLeader;
		private MetadataStoreStub stub;
		private int lastLog;

		public boolean isLeader() {
			return isLeader;
		}

		public void setLeader(boolean isLeader) {
			this.isLeader = isLeader;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public MetadataStoreStub getStub() {
			if(stub == null) {
				try {
					stub = MetadataStoreGrpc.newStub(
							ManagedChannelBuilder.forAddress("127.0.0.1", this.port).usePlaintext(true).build());					
				}
				catch(Exception e) {
					logger.log(Level.SEVERE, "Cannot create stub for:" + this.port);
				}
			}
			return stub;
		}

		public int getLastLog() {
			return this.lastLog;
		}

		public void setLastLog(int index) {
			this.lastLog = index;
		}

		public NodeInfo(int port, boolean isLeader) {
			this.port = port;
			this.isLeader = isLeader;
			this.stub = null;
			this.lastLog = -1;
		}
	}

	static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
	    private static final Logger logger = Logger.getLogger(MetadataStoreImpl.class.getName());

		private Map<String, FileInfo> fileInfos;
		private List<FileInfo> logList;
		private int lastCommitted;
		private BlockStoreBlockingStub blockStoreStub;
		private boolean isLeader;
		private List<NodeInfo> nodeList;
		private int leaderPort;
		private boolean isUp;
		
		public class VoteCounter{
			private int counter = 0;
			
			public int getCounter() {
				return counter;
			}
			
			public void tick() {
				counter++;
			}
		}
		
		public class HeartbeatThread extends Thread{

			@Override
			public void run() {
				while(true) {
					for(NodeInfo node: nodeList) {
						LogMessage.Builder builder = LogMessage.newBuilder();
						if(node.lastLog < logList.size() - 1) {
							int nextIndex = node.lastLog + 1;
							builder.setLogIndex(nextIndex);
							for(int i = nextIndex; i < logList.size(); i++) {
								builder.addLogContent(logList.get(i));
							}
						}
						sendAppendLog(builder.build(), null, node);
					}
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						logger.log(Level.INFO, "Interrupted");
					}
				}
			}
			
		}

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

			for (int i = 1; i <= config.getNumMetadataServers(); i++) {
				// add all other nodes that aren't leader
				if (i != nodeNumber) {
					nodeList.add(new NodeInfo(config.getMetadataPort(i), false));
				}
			}

			this.fileInfos = new HashMap<String, FileInfo>();
			
			if(this.isLeader) {
				new HeartbeatThread().start();
			}
		}

		@Override
		public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
			Empty response = Empty.newBuilder().build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#readFile(surfstore.
		 * SurfStoreBasic.FileInfo, io.grpc.stub.StreamObserver)
		 */
		/*
		 * (non-Javadoc)
		 * 
		 * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#readFile(surfstore.
		 * SurfStoreBasic.FileInfo, io.grpc.stub.StreamObserver)
		 */
		/*
		 * (non-Javadoc)
		 * 
		 * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#readFile(surfstore.
		 * SurfStoreBasic.FileInfo, io.grpc.stub.StreamObserver)
		 */
		@Override
		public void readFile(FileInfo request, StreamObserver<FileInfo> responseObserver) {
			String requestedFilename = request.getFilename();

			FileInfo responseFile;
			if (fileInfos.containsKey(requestedFilename)) {
				responseFile = fileInfos.get(requestedFilename);
			} else {
				responseFile = FileInfo.newBuilder().setFilename("").setFilename(requestedFilename).setVersion(0)
						.build();
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

			if (!this.isLeader) {
				builder.setResult(WriteResult.Result.NOT_LEADER);
				builder.setCurrentVersion(currentVersion);
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				return;
			}

			// Leader Initialize 2PC
			synchronized (logList) {
				if (newVersion != currentVersion + 1) {
					builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(currentVersion);
				} else {
					for (String hash : hashList) {
						Block block = Block.newBuilder().setHash(hash).build();
						if (!this.blockStoreStub.hasBlock(block).getAnswer()) {
							builder.addMissingBlocks(hash);
							hasNewBlock = true;
						}
					}
					
					if (hasNewBlock) {
						// blocks not exist in the block store
						builder.setResult(WriteResult.Result.MISSING_BLOCKS);
						builder.setCurrentVersion(currentVersion);
					} else {
						// send append request
						VoteCounter counter = new VoteCounter();
						counter.tick();
						LogMessage.Builder logMsgBuilder = LogMessage.newBuilder().setLogIndex(logList.size()).addLogContent(request);
						logList.add(request);
						
						for(NodeInfo node: nodeList) {						
							sendAppendLog(logMsgBuilder.build(), counter, node);
						}
						
						int waitTime = 0;
						while(counter.getCounter() * 2 <= nodeList.size() && waitTime < 1000) {
							try {
								Thread.sleep(10);
								waitTime += 10;
							}
							catch(Exception e) {
								logger.log(Level.INFO, "Thread interrupted");
							}
						}
						
						if(counter.getCounter() * 2 > nodeList.size()) {
							commitNextLog();
							builder.setResult(WriteResult.Result.OK);
							builder.setCurrentVersion(newVersion);
							
							for(NodeInfo node: nodeList) {
								sendCommitLog(CommitMessage.newBuilder().setCommitIndex(lastCommitted).build(), node);
							}
						}
					}
				}
			}

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		private void sendAppendLog(final LogMessage msg, final VoteCounter counter, final NodeInfo nodeInfo) {
			StreamObserver<LogAnswer> observer = new StreamObserver<LogAnswer>() {
				@Override
				public void onNext(LogAnswer answer) {
					if(answer.getMostRecentLog() >= msg.getLogIndex()) {
						if(counter != null) {
							counter.tick();
						}
						nodeInfo.setLastLog(Math.max(nodeInfo.lastLog, answer.getMostRecentLog()));
					}
				}

				@Override
				public void onError(Throwable t) {
				}

				@Override
				public void onCompleted() {
				}
			};
			try {
				nodeInfo.getStub().appendLog(msg, observer);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Send append log failed:" + nodeInfo.getPort());
			}
		}
		
		private void sendCommitLog(final CommitMessage msg, final NodeInfo nodeInfo) {
			StreamObserver<Empty> observer = new StreamObserver<Empty>() {
				@Override
				public void onNext(Empty answer) {
				}

				@Override
				public void onError(Throwable t) {
				}

				@Override
				public void onCompleted() {
				}
			};
			try {
				nodeInfo.getStub().commitLog(msg, observer);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Send commit log failed:" + nodeInfo.getPort());
			}
		}


		@Override
		public void deleteFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
			int newVersion = request.getVersion();

			WriteResult.Builder builder = WriteResult.newBuilder();
			int currentVersion = getCurrentVersion(request);
			
			if (!this.isLeader) {
				builder.setResult(WriteResult.Result.NOT_LEADER);
				builder.setCurrentVersion(currentVersion);
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				return;
			}

			// Leader Initialize 2PC
			synchronized(logList) {
				if (newVersion != currentVersion + 1) {
					builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(currentVersion);
				} else {
					FileInfo log = FileInfo.newBuilder().setFilename(request.getFilename()).setVersion(newVersion)
							.addBlocklist("0").build();
					// append log
					// send append request
					VoteCounter counter = new VoteCounter();
					counter.tick();
					LogMessage.Builder logMsgBuilder = LogMessage.newBuilder().setLogIndex(logList.size()).addLogContent(log);
					logList.add(log);
					
					for(NodeInfo node: nodeList) {						
						sendAppendLog(logMsgBuilder.build(), counter, node);
					}
					
					int waitTime = 0;
					while(counter.getCounter() * 2 <= nodeList.size() && waitTime < 1000) {
						try {
							Thread.sleep(10);
							waitTime += 10;
						}
						catch(Exception e) {
							
						}
					}
					
					if(counter.getCounter() * 2 > nodeList.size()) {
						commitNextLog();
						builder.setResult(WriteResult.Result.OK);
						builder.setCurrentVersion(newVersion);
						
					}
				}
			}

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		private int getCurrentVersion(FileInfo request) {
			String requestedFilename = request.getFilename();
			int currentVersion = 0;
			if (fileInfos.containsKey(requestedFilename)) {
				FileInfo existingFileInfo = fileInfos.get(requestedFilename);
				currentVersion = existingFileInfo.getVersion();
			}
			return currentVersion;
		}

		private void commitNextLog() {
			lastCommitted += 1;
			FileInfo log = logList.get(lastCommitted);
			fileInfos.put(log.getFilename(), log);
		}

		private void printFileSummary() {
			System.out.println("File Summary");

			for (FileInfo f : this.fileInfos.values()) {
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
			if (this.getCurrentVersion(request) != 0) {
				ans = fileInfos.get(request.getFilename());
			} else {
				ans = FileInfo.newBuilder().setFilename(request.getFilename()).setVersion(0).build();
			}
			responseObserver.onNext(ans);
			responseObserver.onCompleted();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see surfstore.MetadataStoreGrpc.MetadataStoreImplBase#appendLog(surfstore.
		 * SurfStoreBasic.LogMessage, io.grpc.stub.StreamObserver) rpc for append log.
		 * The leader sends a list of consecutive logs from a point of time. The index
		 * sent in the request is the log index of the first log in the list. Client
		 * response is the index of latest log after the log is appended. In the case
		 * log cannot be appended, the number is the latest log that client has.
		 * Rejection is assumed if the response index does not match the latest index of
		 * the log sent.
		 */
		@Override
		public void appendLog(LogMessage request, StreamObserver<LogAnswer> responseObserver) {
			int newLogIndex = request.getLogIndex();
			int expectedLog = logList.size();
			LogAnswer.Builder builder = LogAnswer.newBuilder();
			if (newLogIndex > expectedLog || !isUp) {
				// reject log
				builder.setMostRecentLog(expectedLog - 1);
			} else {
				int indexToAppend = expectedLog - newLogIndex;
				List<FileInfo> newLogs = request.getLogContentList();
				for (int i = indexToAppend; i < newLogs.size(); i++) {
					logList.add(newLogs.get(i));
				}
				builder.setMostRecentLog(logList.size() - 1);
			}
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		@Override
		public void commitLog(CommitMessage request, StreamObserver<Empty> responseObserver) {
			if(isUp) {
				int commitVersion = request.getCommitIndex();
				while (lastCommitted < commitVersion && logList.size() > commitVersion) {
					commitNextLog();
				}				
			}
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		}
	}
}