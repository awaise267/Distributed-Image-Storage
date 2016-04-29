/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.client;

import com.google.protobuf.ByteString;
import pipe.common.Common.Header;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.io.MergedStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;

import routing.Pipe.CheckRequest;
import routing.Pipe.CommandMessage;
import routing.Pipe.GetRequest;
import routing.Pipe.ResponseMsg;
import routing.Pipe.ResponseMsg.Operation;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient implements Runnable {
	// track requests
	private long curID = 0;
	private volatile int LeaderId = -1;
	private volatile String LeaderHost;
	private volatile int LeaderPort = -1;
	private HashMap<Long, HashMap<Integer, byte[]>> filesMap = new HashMap<Long, HashMap<Integer, byte[]>>();
	private HashMap<Integer, CommConnection> connectionList = new HashMap<Integer, CommConnection>();

	public MessageClient(String host, int port, int nodeId) {
		init(host, port, nodeId);
	}

	private void init(String host, int port, int nodeId) {
		connectionList.put(nodeId, new CommConnection(host, port, this));
	}

	public long sendImage(String path, CommConnection Conn) throws IOException {
		long fileId = System.currentTimeMillis();
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		try {
			List<byte[]> arrayList = new ArrayList<byte[]>();
			Path file = Paths.get(path);
			byte[] buf = Files.readAllBytes(file);
			arrayList = FileUtil.divideArray(buf, 64 * 1024);
			int count = 1;
			for (byte[] a : arrayList) {
				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(290);
				hb.setTime(System.currentTimeMillis());
				hb.setDestination(-1);

				rb.setHeader(hb);
				rb.setFileId(fileId);
				rb.setChunkId(count);
				rb.setSize((long) arrayList.size());
				rb.setFile(ByteString.copyFrom(a));
				Conn.getInstance().getChannel().writeAndFlush(rb.build());
				System.out.println("File size is " + arrayList.size());
				System.out.println("Sent chunk : " + count + " of file Id : " + fileId + " to the server");
				count++;
				Thread.sleep(200);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileId;
	}

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	private synchronized long nextId() {
		return ++curID;
	}

	public void getLeader(CommConnection conn) {

		Header.Builder hb = Header.newBuilder();
		hb.setDestination(-1);
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());

		LeaderStatus.Builder ls = LeaderStatus.newBuilder();
		ls.setAction(LeaderQuery.WHOISTHELEADER);

		CommandMessage.Builder cmdBuilder = CommandMessage.newBuilder();
		cmdBuilder.setLeaderStatus(ls);
		cmdBuilder.setHeader(hb);

		try {
			conn.getChannel().writeAndFlush(cmdBuilder.build());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void displayMenu() {
		try {
			int choice = -1;
			while (choice != 4) {
				System.out.println("Menu");
				System.out.println("1. Put File");
				System.out.println("2. Get File");
				System.out.println("3. Search For File");				
				System.out.print("Enter your choice: ");
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				choice = Integer.parseInt(br.readLine());
				switch (choice) {
				case 1:
					System.out.print("Enter the path of the file : ");
					String path = br.readLine();
					put(path);
					break;
				case 2:
					System.out.print("Enter the file Id : ");
					String no = br.readLine();
					long getFileId = Long.valueOf(no).longValue();
					get(getFileId);
					System.out.println("Getting file from server. Response will be displayed on the console.");
					break;
				case 3:
					System.out.print("Enter the file Id : ");
					String no2 = br.readLine();
					long getFileId2 = Long.valueOf(no2).longValue();

					check(getFileId2);
					System.out.println("Checking file in server. Response will be displayed on the console.");
					break;
				default:
					System.out.println("Invalid Choice!");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void put(String path) throws Exception {
		if (connectionList.containsKey(LeaderId)) {
			sendImage(path, connectionList.get(LeaderId));
		} else {
			init(LeaderHost, LeaderPort + 1, LeaderId);
			sendImage(path, connectionList.get(LeaderId));
		}
		System.out.println("Sending file to server. Response will be displayed on console.");
		return;
	}

	public void get(long fileId) throws Exception {
		if (connectionList.containsKey(LeaderId)) {
			getFile((long) fileId, connectionList.get(LeaderId));
		} else {
			init(LeaderHost, LeaderPort + 1, LeaderId);
			getFile((long) fileId, connectionList.get(LeaderId));
		}
	}

	public void check(long fileId) throws Exception {
		System.out.println("in check method for " + fileId);
		if (connectionList.containsKey(LeaderId)) {
			checkFile((long) fileId, connectionList.get(LeaderId));
		} else {

			init(LeaderHost, LeaderPort + 1, LeaderId);
			checkFile((long) fileId, connectionList.get(LeaderId));
		}
	}

	public void getFile(long fileId, CommConnection Conn) {
		CommandMessage.Builder wb = CommandMessage.newBuilder();

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		GetRequest.Builder gb = GetRequest.newBuilder();
		gb.setFileId(fileId);

		wb.setHeader(hb);
		wb.setGetRequest(gb);

		Conn.getInstance().getChannel().writeAndFlush(wb.build());
	}

	public void checkFile(long fileId, CommConnection Conn) {
		CommandMessage.Builder wb = CommandMessage.newBuilder();

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		CheckRequest.Builder cb = CheckRequest.newBuilder();
		cb.setFileId(fileId);

		wb.setHeader(hb);
		wb.setCheckRequest(cb);

		Conn.getInstance().getChannel().writeAndFlush(wb.build());
		System.out.println("sent check request for  " + fileId);
	}

	public synchronized void returnLeaderId(CommandMessage msg) throws IOException {
		LeaderId = msg.getLeaderStatus().getLeaderId();
		LeaderHost = msg.getLeaderStatus().getLeaderHost();
		LeaderPort = msg.getLeaderStatus().getLeaderPort();
		System.out
				.println("Leader Id : " + LeaderId + ". Leader Host : " + LeaderHost + ". Leader Port : " + LeaderPort);

		displayMenu();
	}

	public void serverResponseHandler(CommandMessage msg) {
		ResponseMsg resMsg = msg.getReponseMsg();
		if (resMsg.getOperation() == Operation.Get) {
			if (resMsg.hasFileMsg() && resMsg.getResponseStatus()) {
				if (!filesMap.containsKey(Long.valueOf(resMsg.getFileId()))) {
					HashMap<Integer, byte[]> chunkList = new HashMap<Integer, byte[]>();
					chunkList.put(resMsg.getFileMsg().getChunkId(), resMsg.getFileMsg().getFileChunk().toByteArray());
					
					if (chunkList.size() == resMsg.getFileMsg().getSize()) {
						mergeChunks(chunkList, Long.toString(resMsg.getFileId()));
					} else {
						filesMap.put(resMsg.getFileId(), chunkList);
					}
				} else {
					HashMap<Integer, byte[]> chunkList = filesMap.get(Long.valueOf(resMsg.getFileId()));
					chunkList.put(resMsg.getFileMsg().getChunkId(), resMsg.getFileMsg().getFileChunk().toByteArray());
					if (chunkList.size() == resMsg.getFileMsg().getSize()) {
						mergeChunks(chunkList, Long.toString(resMsg.getFileId()));
						filesMap.remove(Long.valueOf(resMsg.getFileId()));
					}
				}
				// call merge file here
			} else {
				System.out.println("\nUnable to retrieve file with Id: " + resMsg.getFileId() + " from the database");
			}
		} else if (resMsg.getOperation() == Operation.Put) {
			if (resMsg.getResponseStatus())
				System.out.println(
						"\nFile with file Id : " + resMsg.getFileId() + " successfully entered into the database");
			else
				System.out.println("\nUnable to put file with Id: " + resMsg.getFileId() + " into the database");
		} else {
			if (resMsg.getResponseStatus()) {
				System.out.println("\nFile with file Id : " + resMsg.getFileId() + " is present in the database");
			} else {
				System.out.println("\nCould not find file with file Id : " + resMsg.getFileId() + " in the database");
			}
		}

	}

	public void mergeChunks(HashMap<Integer, byte[]> chunkList, String fileId) {
		try {
			
			
			String filePath = "runtime/client_runtime/" + fileId+".jpg";
			File out = new File(filePath);
			FileOutputStream fos = new FileOutputStream(out);
			for (byte[] b : chunkList.values()) {
				fos.write(b);
			}
			System.out.println("\nFile retrieved in : "+out.getAbsolutePath());
			fos.close();
			displayMenu();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

		while (this.LeaderHost == "" || this.LeaderHost == null || this.LeaderId == -1 || this.LeaderPort == -1) {
			getLeader(connectionList.get(1));
			System.out.println("Waiting for leader to be returned by server");
			try {
				System.out.println(LeaderHost+","+LeaderId+","+LeaderPort);
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
