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
package gash.router.server;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gash.router.container.RoutingConf;
import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.election.RaftManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import routing.Pipe.CommandMessage;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	RaftManager manager;
	List<byte[]> arrayListRes = new ArrayList<byte[]>();

	public CommandHandler(RoutingConf conf, RaftManager mgr) {
		if (conf != null) {
			this.manager = mgr;
			this.conf = conf;
		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		// PrintUtil.printCommand(msg);

		try {
			// TODO How can you implement this without if-else statements?
			if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			} else if (msg.hasFile()) {
				System.out.println("File received in CommandHandler");
				manager.receiveFile(msg, channel);

			} else if (msg.hasLeaderStatus()) {
				CommandMessage.Builder cmdBldr = CommandMessage.newBuilder();

				Header.Builder hb = Header.newBuilder();
				hb.setDestination(999);
				hb.setNodeId(manager.getNodeId());
				hb.setTime(System.currentTimeMillis());
				cmdBldr.setHeader(hb);
				System.out.println("getting leader status");
				if (msg.getLeaderStatus().getAction() == LeaderQuery.WHOISTHELEADER) {
					LeaderStatus.Builder leaderStatusMessage = LeaderStatus.newBuilder();
					if (manager.getLeaderId() != -1) {
						leaderStatusMessage.setAction(LeaderQuery.THELEADERIS);

						pipe.election.Election.LeaderStatus.LeaderState lst = pipe.election.Election.LeaderStatus.LeaderState.LEADERALIVE;
						leaderStatusMessage.setState(lst);
						if (manager.getLeaderId() == manager.getNodeId()) {
							leaderStatusMessage.setLeaderHost(manager.getSelfHost());
							leaderStatusMessage.setLeaderId(manager.getLeaderId());
							leaderStatusMessage.setLeaderPort(manager.getRoutingConf().getCommandPort());
						} else {
							for (RoutingEntry routeEntry : manager.getRoutingConf().getRouting()) {
								if (routeEntry.getId() == manager.getLeaderId()) {
									leaderStatusMessage.setLeaderHost(routeEntry.getHost());
									leaderStatusMessage.setLeaderId(manager.getLeaderId());
									leaderStatusMessage.setLeaderPort(routeEntry.getPort());
								}
							}
						}
						// for(EdgeInfo ei :manager.get)
					} else {
						leaderStatusMessage.setAction(LeaderQuery.THELEADERIS);
						pipe.election.Election.LeaderStatus.LeaderState lst = pipe.election.Election.LeaderStatus.LeaderState.LEADERUNKNOWN;
						leaderStatusMessage.setState(lst);
					}
					cmdBldr.setLeaderStatus(leaderStatusMessage);
					channel.writeAndFlush(cmdBldr.build());
					System.out.println("Sending the leader status");
				}
			} else if (msg.hasCheckRequest()) {
				System.out.println("Check request received for file "+msg.getCheckRequest().getFileId() );
				manager.checkForFile(msg, channel);
			} else if (msg.hasGetRequest()) {
				manager.getFile(msg, channel);
			} else {
				// send msg to client with leader node or leader port
			}

		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			// eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
			System.out.println("error");
			e.printStackTrace();
		}

		System.out.flush();
	}

	public void storeChunks(CommandMessage msg) {

		if (arrayListRes.size() == msg.getSize()) {
			// to merge files to Image - mergeFiles(arrayListRes);
		}
	}

	public void mergeFiles() {
		FileOutputStream fos;
		try {
			fos = new FileOutputStream("C:\\Users\\SCS_USER\\Downloads\\Test2.jpg");
			for (byte[] b : arrayListRes)
				fos.write(b);
			fos.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// ImageIO is a class containing static methods for locating
		// ImageReaders
		// and ImageWriters, and performing simple encoding and decoding.

	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// logger.error("Unexpected exception from downstream Command.", cause);
		System.out.println("A client disconnected");
		ctx.close();
	}

}