package gash.router.server.election;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import com.google.protobuf.ByteString;
import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.database.DatabaseConnection;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import io.netty.channel.Channel;
import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;
import routing.FileTransfer.FileMsg;
import routing.FileTransfer.LogResponse;
import routing.FileTransfer.RaftMessage;
import routing.Pipe.CommandMessage;
import routing.Pipe.ResponseMsg;
import routing.Pipe.ResponseMsg.Operation;

public class RaftManager implements Runnable {
	private ServerState state;
	private int nodeId = -1;
	private int leaderId = -1;
	public Channel clientChannel;

	private String selfHost;
	private RoutingConf conf;
	private EdgeMonitor emon;

	private long timerStart = 0;
	// This servers states
	private volatile RaftState CurrentState;
	public RaftState Leader;
	public RaftState Candidate;
	public RaftState Follower;

	private int heartBeatBase = 3000;
	private volatile long electionTimeout = 3000;
	private volatile long lastKnownBeat = 0;
	private Random rand;
	private int term = 0;
	// private int commitIndex = 0;

	private HashMap<Long, LogEntry> TempLogEntries = new HashMap<Long, LogEntry>();
	private HashMap<Long, LogEntry> LogEntries = new HashMap<Long, LogEntry>();
	private HashMap<Long, Integer> CommittedLogs = new HashMap<Long, Integer>();

	public RaftManager(ServerState state) {
		this.state = state;
	}

	public void init() throws UnknownHostException {
		selfHost = Inet4Address.getLocalHost().getHostAddress();

		rand = new Random();
		Leader = new LeaderState();
		Leader.setManager(this);

		Candidate = new CandidateState();
		Candidate.setManager(this);

		Follower = new FollowerState();
		Follower.setManager(this);

		this.conf = state.getConf();
		this.emon = state.getEmon();

		lastKnownBeat = System.currentTimeMillis();
		heartBeatBase = conf.getHeartbeatDt();
		nodeId = conf.getNodeId();

		randomizeElectionTimeout();
		electionTimeout += 1000;

		CurrentState = Follower;
	}

	@Override
	public void run() {
		while (true) {
			timerStart = System.currentTimeMillis();
			CurrentState.process();
		}

	}

	/* Start: Code to put file in server */

	public synchronized void receiveFile(CommandMessage msg, Channel channel) {
		System.out.println("File received by raft manager in receive file");
		clientChannel = channel;
		ByteString s = msg.getFile();
		byte[] chunk = s.toByteArray();
		LogEntry log;
		if (TempLogEntries.containsKey(msg.getFileId())) {
			log = TempLogEntries.get(msg.getFileId());
			log.addChunk(msg.getChunkId(), chunk);
		} else {
			log = new LogEntry(msg.getFileId(), (int) msg.getSize(), channel);
			log.addChunk(msg.getChunkId(), chunk);
			TempLogEntries.put(msg.getFileId(), log);
		}
		if (log.isComplete) {
			System.out.println("Log Complete");
			TempLogEntries.remove(log.fileId);
			System.out.println("Removed log from temp entries");
			LogEntries.put(log.fileId, log);
			System.out.println("Put log in Log entries");
			sendToAllServers(log);
			System.out.println("File sent to all serveers");
		}
	}

	public synchronized void sendToAllServers(LogEntry log) {
		for (EdgeInfo e : emon.getOutBoundList().getEdgeMap().values()) {
			if (e.isActive() && e.getChannel() != null) {

				for (int chunkId : log.ChunkList.keySet()) {
					try {
						FileMsg.Builder fb = FileMsg.newBuilder();
						fb.setChunkId(chunkId);
						fb.setFileId(log.fileId);
						fb.setSize(log.fileSize);
						fb.setFileChunk(ByteString.copyFrom(log.ChunkList.get(chunkId)));

						Header.Builder hb = Header.newBuilder();
						hb.setDestination(-1);
						hb.setNodeId(this.nodeId);
						hb.setTime(System.currentTimeMillis());

						WorkMessage.Builder wb = WorkMessage.newBuilder();
						wb.setHeader(hb);
						wb.setFileMessage(fb);
						wb.setSecret(1);
						e.getChannel().writeAndFlush(wb.build());
					} catch (Exception e1) {
						e1.printStackTrace();
					} finally {
						log.setSentCount(log.getSentCount() + 1);
					}
				}
			}
		}
	}

	public synchronized void receiveLogEntries(WorkMessage msg, Channel channel) {
		if (this.CurrentState != this.Leader) {
			ByteString s = msg.getFileMessage().getFileChunk();
			byte[] chunk = s.toByteArray();
			LogEntry log;
			if (TempLogEntries.containsKey(msg.getFileMessage().getFileId())) {
				log = TempLogEntries.get(msg.getFileMessage().getFileId());
				log.addChunk(msg.getFileMessage().getChunkId(), chunk);
			} else {
				log = new LogEntry(msg.getFileMessage().getFileId(), (int) msg.getFileMessage().getSize(), channel);
				log.addChunk(msg.getFileMessage().getChunkId(), chunk);
				TempLogEntries.put(msg.getFileMessage().getFileId(), log);
			}
			System.out.println("Files being received by Node " + this.nodeId);
			if (log.isComplete) {
				sendLogResponse(log);
				System.out.println("Internal node has received the complete log!");
			}
		}
	}

	public synchronized void sendLogResponse(LogEntry log) {
		for (EdgeInfo e : emon.getOutBoundList().getEdgeMap().values()) {
			if (e.isActive() && e.getChannel() != null && this.leaderId == e.getRef()) {
				RaftMessage.Builder rb = RaftMessage.newBuilder();

				LogResponse.Builder lb = LogResponse.newBuilder();
				lb.setFileIdReplicated(log.fileId);
				rb.setLogResponse(lb);

				Header.Builder hb = Header.newBuilder();
				hb.setDestination(-1);
				hb.setNodeId(this.nodeId);
				hb.setTime(System.currentTimeMillis());

				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setHeader(hb);
				wb.setRaftMessage(rb);
				wb.setSecret(1);
				e.getChannel().writeAndFlush(wb.build());
				System.out.println("Sent confirmation back to the leader.");
			}
		}

		// TODO Get the database used by current node from the config file.
		// Currently using hard coded values (Nodes 1 & 2 store to Mongo and 5 &
		// 6 to mysql)
		try {
			for (int key : log.ChunkList.keySet()) {
				if (nodeId == 1 || nodeId == 2) {
					DatabaseConnection.putFilesToMongo(log.fileId, key, log.ChunkList.get(key));
				} else {
					DatabaseConnection.insertToDatabase(log.fileId, key, log.ChunkList.get(key));
				}
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		CommittedLogs.put(log.fileId, this.getTerm());
		LogEntries.remove(log.fileId);

	}

	public synchronized void raftMessageHandler(WorkMessage msg, Channel channel) {
		RaftMessage raftMsg = msg.getRaftMessage();
		if (raftMsg.hasLogResponse()) {
			handleLogResponse(raftMsg, channel);
		}
	}

	public synchronized void handleLogResponse(RaftMessage msg, Channel channel) {
		// TODO commit file replicated to db
		if (this.CurrentState == this.Leader) {
			try {
				long fileReplicated = msg.getLogResponse().getFileIdReplicated();
				LogEntry log = LogEntries.get(fileReplicated);
				if (!CommittedLogs.containsKey(fileReplicated) && !log.isCompletelyReplicated()) {
					log.setReplicatedCount(log.getReplicatedCount() + 1);
					System.out.println(fileReplicated + " file replicated to " + log.getReplicatedCount() + " out of "
							+ log.getSentCount() / log.fileSize + " servers");
					if (log.getReplicatedCount() > (log.getSentCount() / (2 * log.fileSize))
							&& !log.isCompletelyReplicated()) {
						Iterator it = log.ChunkList.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry pair = (Map.Entry) it.next();

							
							// TODO Get the database used by current node from the config file.
							// Currently using hard coded values (Nodes 1 & 2 store to Mongo and 5 &
							// 6 to mysql)
							
							if (nodeId == 1 || nodeId == 2) {
								DatabaseConnection.putFilesToMongo(log.fileId, ((Integer) pair.getKey()).intValue(),
										(byte[]) pair.getValue());
							} else {
								DatabaseConnection.insertToDatabase(log.fileId, ((Integer) pair.getKey()).intValue(),
										(byte[]) pair.getValue());
							}

						}
						log.setCompletelyReplicated(true);
						CommittedLogs.put(fileReplicated, this.getTerm());
						LogEntries.remove(fileReplicated);
						sendResponseToClient(fileReplicated, true, Operation.Put, clientChannel);
						System.out.println("response sent to client");
					}
				}
				return;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/* End: Code to put file in server */

	/* Start: Code to get file from server */

	public synchronized void getFile(CommandMessage msg, Channel channel) {
		if (CommittedLogs.containsKey(msg.getGetRequest().getFileId())) {
			
			// TODO Get the database used by current node from the config file.
			// Currently using hard coded values (Nodes 1 & 2 store to Mongo and 5 &
			// 6 to mysql)
			if (nodeId == 1 || nodeId == 2) {
				DatabaseConnection.sendFilesFromMongo(msg.getGetRequest().getFileId(), channel);
			} else {
				DatabaseConnection.sendFilesFromMySQL(msg.getGetRequest().getFileId(), channel);
			}
			
		} else {
			sendResponseToClient(msg.getGetRequest().getFileId(), false, Operation.Get, channel);
		}
	}

	/* End: Code to get file from server */

	/* Start: Code to check if file is present */
	public synchronized void checkForFile(CommandMessage msg, Channel channel) {
		long fileId = msg.getCheckRequest().getFileId();
		System.out.println("Checking for file id " + fileId);
		if (CommittedLogs.containsKey(fileId)) {
			sendResponseToClient(fileId, true, Operation.Find, channel);
		} else {
			sendResponseToClient(fileId, false, Operation.Find, channel);
		}
	}
	/* End: Code to check if file is present */

	public synchronized void sendResponseToClient(long fileId, boolean status, Operation operation, Channel channel) {
		ResponseMsg.Builder rmb = ResponseMsg.newBuilder();

		rmb.setFileId(fileId);
		rmb.setResponseStatus(status);

		if (operation == Operation.Put)
			rmb.setOperation(Operation.Put);
		else if (operation == Operation.Get)
			rmb.setOperation(Operation.Get);
		else if (operation == Operation.Find)
			rmb.setOperation(Operation.Find);

		Header.Builder hb = Header.newBuilder();
		hb.setDestination(-1);
		hb.setNodeId(this.nodeId);
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.setHeader(hb);
		cb.setReponseMsg(rmb);

		channel.writeAndFlush(cb.build());
		System.out.println(channel);

	}

	public String getSelfHost() {
		return selfHost;
	}

	public synchronized RaftState getCurrentState() {
		return CurrentState;
	}

	public synchronized long getTimerStart() {
		return timerStart;
	}

	public synchronized void setTimerStart(long t) {
		timerStart = t;
	}

	public synchronized void setCurrentState(RaftState st) {
		CurrentState = st;
	}

	public synchronized int getNodeId() {
		return nodeId;
	}

	public synchronized int getLeaderId() {
		return leaderId;
	}

	public synchronized void setLeaderId(int id) {
		leaderId = id;
	}

	public synchronized RoutingConf getRoutingConf() {
		return conf;
	}

	public synchronized EdgeMonitor getEmon() {
		return emon;
	}

	public synchronized int getTerm() {
		return term;
	}

	public synchronized void setTerm(int trm) {
		term = trm;
	}

	public synchronized long getLastKnownBeat() {
		return lastKnownBeat;
	}

	public synchronized void setLastKnownBeat(long beatTime) {
		lastKnownBeat = beatTime;
	}

	public synchronized void setState(ServerState state) {
		this.state = state;
	}

	public ServerState getState() {
		if (state != null)
			return state;
		else
			return null;
	}

	public synchronized void setElectionTimeout(long et) {
		electionTimeout = et;
	}

	public synchronized void randomizeElectionTimeout() {

		int temp = rand.nextInt(heartBeatBase);
		temp = temp + heartBeatBase;
		electionTimeout = (long) temp;

		// System.out.println("Randomized Timeout value is : "+electionTimeout);
	}

	public synchronized long getElectionTimeout() {
		return electionTimeout;
	}

	public synchronized int getHbBase() {
		return heartBeatBase;
	}
}
