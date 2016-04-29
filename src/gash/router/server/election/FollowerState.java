package gash.router.server.election;

import gash.router.server.edges.EdgeInfo;
import pipe.common.Common.Header;
import pipe.work.Work.VoteMessage;
import pipe.work.Work.WorkMessage;

public class FollowerState implements RaftState {

	private RaftManager Manager;
	private int votedFor = -1;

	@Override
	public synchronized void process() {
		try {
			if (Manager.getElectionTimeout() <= 0 && (System.currentTimeMillis() - Manager.getLastKnownBeat() > Manager.getHbBase())) {
				Manager.setCurrentState(Manager.Candidate);
				return;
			} else {
				Thread.sleep(200);
				long dt = Manager.getElectionTimeout() - (System.currentTimeMillis() - Manager.getTimerStart());
				Manager.setElectionTimeout(dt);				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void setManager(RaftManager Mgr) {
		this.Manager = Mgr;
	}

	@Override
	public synchronized RaftManager getManager() {
		return Manager;
	}

	@Override
	public synchronized void receivedVote(WorkMessage msg) {
		// Do Nothing
		return;
	}

	@Override
	public synchronized void voteRequested(WorkMessage msg) {
		System.out.println("Vote requested by "+msg.getHeader().getNodeId());
		if (msg.getRequestVote().getTerm() > Manager.getTerm()) {
			votedFor = -1;

			if (votedFor == -1) {
				votedFor = msg.getHeader().getNodeId();
				Manager.randomizeElectionTimeout();
				Manager.setTerm(Manager.getTerm() + 1);
				System.out.println(System.currentTimeMillis() + " : You voted for " + votedFor + " in term "
						+ Manager.getTerm() + ". Timeout is : " + Manager.getElectionTimeout());

				replyVote(msg, true);
			} else {
				replyVote(msg, false);
			}
		} else {
			replyVote(msg, false);
		}

		// if(msg.getRequestVote().getTerm() == Manager.getTerm()){
		// // in case new election has started but we have not voted for anyone
		// if(votedFor == -1){
		// votedFor = msg.getHeader().getNodeId();
		// Manager.randomizeElectionTimeout();
		// System.out.println(System.currentTimeMillis() + " : You voted for
		// "+votedFor+" in term "+Manager.getTerm());
		// Manager.randomizeElectionTimeout(); // randomize only if we vote for
		// the candidate that sent the message.
		// replyVote(msg, true);
		// }else{
		// replyVote(msg, false);
		// }
		// }

	}

	@Override
	public synchronized void replyVote(WorkMessage msg, boolean voteGranted) {
		int toNodeId = msg.getHeader().getNodeId();
		int fromNodeId = Manager.getRoutingConf().getNodeId();
		EdgeInfo ei = Manager.getEmon().getOutBoundList().getEdgeMap().get(toNodeId);

		if (ei.isActive() && ei.getChannel() != null) {
			VoteMessage.Builder vb = VoteMessage.newBuilder();
			vb.setTerm(Manager.getTerm());
			vb.setVoteGranted(voteGranted);

			Header.Builder hb = Header.newBuilder();
			hb.setNodeId(fromNodeId);
			hb.setDestination(-1);
			hb.setTime(System.currentTimeMillis());

			WorkMessage.Builder wb = WorkMessage.newBuilder();
			wb.setVoteMessage(vb);
			wb.setHeader(hb);
			wb.setSecret(1);
			ei.getChannel().writeAndFlush(wb.build());
		}

	}

	@Override
	public synchronized void sendAppendMessage() {
		// TODO Auto-generated method stub

	}

	@Override
	public synchronized void getAppendMessage(WorkMessage msg) {
		// TODO Auto-generated method stub
		if (msg.getAppendMessage().getTerm() >= Manager.getTerm()) {
			votedFor = -1;

			Manager.randomizeElectionTimeout();
			Manager.randomizeElectionTimeout();
			if (msg.getAppendMessage().getLeaderId() != Manager.getLeaderId())
				System.out.println("Leader heartbeat received. Leader is : " + msg.getAppendMessage().getLeaderId()
						+ " Timeout left is : " + Manager.getElectionTimeout());

			Manager.setTerm((int) msg.getAppendMessage().getTerm());

			Manager.setLeaderId((int) msg.getAppendMessage().getLeaderId());
			Manager.setLastKnownBeat(System.currentTimeMillis());

		}
	}

	@Override
	public synchronized void sendAppendReply(WorkMessage msg, boolean successStatus) {
		// TODO Auto-generated method stub

	}

	@Override
	public synchronized void getAppendReply(WorkMessage msg) {
		// TODO Auto-generated method stub

	}

}
