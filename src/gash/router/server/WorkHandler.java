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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeInfo;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.work.Work.WorkMessage;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ServerState state;
	protected boolean debug = false;

	public WorkHandler(ServerState state) {
		if (state != null) {
			this.state = state;
		}

	}

	public ServerState getServerState() {
		return state;
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public synchronized void handleMessage(WorkMessage msg, Channel channel) {
		// System.out.println("Messsage Received"+ msg);

		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if (debug) {
			System.out.println("Print Work here");
			PrintUtil.printWork(msg);
		}

		// TODO How can you implement this without if-else statements?
		try {
			if (msg.hasBeat()) {
				this.state.getEmon().getOutBoundList().getEdgeMap().get(msg.getHeader().getNodeId()).setActive(true);
			} else if (msg.hasPing()) {
				System.out.println("Has Ping");
				logger.info("ping from " + msg.getHeader().getNodeId());
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				rb.setSecret(1);
				channel.write(rb.build());
			} else if (msg.hasRequestVote()) {
				System.out.println("Vote requested by "+msg.getHeader().getNodeId());
				System.out.println(state.getManager().getCurrentState().getClass().toString());
				state.getManager().getCurrentState().voteRequested(msg);
				
			} else if (msg.hasVoteMessage()) {
				state.getManager().getCurrentState().receivedVote(msg);
			} else if (msg.hasAppendMessage()) {
				state.getManager().getCurrentState().getAppendMessage(msg);
			} else if (msg.hasFileMessage()) {
				state.getManager().receiveLogEntries(msg, channel);
			} else if (msg.hasRaftMessage()) {
				state.getManager().raftMessageHandler(msg, channel);
			}
		} catch (Exception e) {
			// TODO add logging
			e.printStackTrace();
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			rb.setSecret(1);
			channel.write(rb.build());
		}

		System.out.flush();

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
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// logger.error("WorkHandler: A server just disconnected!", cause);
		for (EdgeInfo ei : state.getEmon().getOutBoundList().getEdgeMap().values()) {
			if (ei.getChannel() == ctx.channel()) {
				ei.setActive(false);
				ei.setChannel(null);
			}
		}
		ctx.close();
	}

}