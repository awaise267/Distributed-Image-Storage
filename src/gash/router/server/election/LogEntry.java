package gash.router.server.election;

import io.netty.channel.Channel;
import java.util.HashMap;

public class LogEntry {
	public long fileId;
	public int fileSize;
	public boolean isComplete = false;
	public Channel channel;
	private int sentCount = 0; // The count of servers the file was sent to;
	private int replicatedCount = 0; // The count of servers that have successfully replicated the file.
	private volatile boolean isCompletelyReplicated = false;
	public HashMap<Integer, byte[]> ChunkList = new HashMap<Integer, byte[]>();

	public LogEntry(long fileId, int fileSize, Channel channel) {
		this.fileId = fileId;
		this.fileSize = fileSize;
		this.channel = channel;
	}

	public void addChunk(int chunkId, byte[] chunk) {
		System.out.println("Added Chunk id : " + chunkId + ". File size is " + fileSize);
		ChunkList.put(chunkId, chunk);
		if (ChunkList.size() == fileSize) {
			System.out.println("Chunk is complete!");
			isComplete = true;
		}
	}

	public int getSentCount() {
		return sentCount;
	}

	public void setSentCount(int sentCount) {
		this.sentCount = sentCount;
	}

	public int getReplicatedCount() {
		return replicatedCount;
	}

	public void setReplicatedCount(int replicatedCount) {
		this.replicatedCount = replicatedCount;
	}

	public synchronized boolean isCompletelyReplicated() {
		return isCompletelyReplicated;
	}

	public synchronized void setCompletelyReplicated(boolean isCompletelyReplicate) {
		this.isCompletelyReplicated = isCompletelyReplicate;
	}

}
