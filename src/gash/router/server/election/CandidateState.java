package gash.router.server.election;

import gash.router.server.edges.EdgeInfo;
import pipe.common.Common.Header;
import pipe.work.Work.RequestVote;
import pipe.work.Work.VoteMessage;
import pipe.work.Work.WorkMessage;

public class CandidateState implements RaftState {
	private RaftManager Manager;
	private int voteCount = 0;
	private int votedFor = -1;
	private int totalActive = 1;

	@Override
	public synchronized void process() {
		try {			
			if (Manager.getElectionTimeout() <= 0 && (System.currentTimeMillis() - Manager.getLastKnownBeat() > Manager.getHbBase())) {
				System.out.println("Node : " + Manager.getNodeId() + " timed out");
				requestVote();
				Manager.randomizeElectionTimeout();
				return;
			}else{
				Thread.sleep(200);
				long dt = Manager.getElectionTimeout() - (System.currentTimeMillis() - Manager.getTimerStart());
				Manager.setElectionTimeout(dt);				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public synchronized void requestVote() {

		System.out.println("Timed out : " + Manager.getElectionTimeout());
		Manager.setTerm(Manager.getTerm() + 1);
		totalActive = 1;
		
		// First get total active nodes. Used for counting majority vote
				// (majority vote -> received votes > total / 2)
		for (EdgeInfo ei : Manager.getEmon().getOutBoundList().getEdgeMap().values()) {
			if (ei.isActive() && ei.getChannel() != null) {
				totalActive++;
			}
		}
		
		voteCount = 0;
		votedFor = Manager.getNodeId(); // Vote for self
		voteCount++;		

		for (EdgeInfo ei : Manager.getEmon().getOutBoundList().getEdgeMap().values()) {
			if (ei.isActive() && ei.getChannel() != null) {
				System.out.println("Sending vote request to "+ei.getRef());
				RequestVote.Builder rb = RequestVote.newBuilder();
				rb.setTerm(Manager.getTerm());
				rb.setCandidateId((long) Manager.getNodeId());

				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(Manager.getRoutingConf().getNodeId());
				hb.setDestination(-1);
				hb.setTime(System.currentTimeMillis());

				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setRequestVote(rb);
				wb.setHeader(hb);
				wb.setSecret(1);

				ei.getChannel().writeAndFlush(wb.build());
			}
		}

		return;
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
		System.out.println("Vote Received from : " + msg.getHeader().getNodeId() + "is : "
				+ msg.getVoteMessage().getVoteGranted());
		if (msg.getVoteMessage().getVoteGranted() && msg.getVoteMessage().getTerm() == Manager.getTerm()) {
			voteCount++;		

			if (voteCount > (totalActive / 2)) {
				Manager.randomizeElectionTimeout();
				
				System.out.println(
						"Leader Elected. Node Id : " + Manager.getNodeId() + ". Out of total nodes: " + totalActive);
				Manager.setLeaderId(Manager.getNodeId());
				votedFor = -1;
				voteCount = 0;
				Manager.setCurrentState(Manager.Leader);
			}
		}
		return;
	}

	@Override
	public synchronized void voteRequested(WorkMessage msg) {
		// TODO Auto-generated method stub
		System.out.println("Candidates Vote requested by "+msg.getHeader().getNodeId());
		if (msg.getRequestVote().getTerm() > Manager.getTerm()) {
			votedFor = -1;
			Manager.randomizeElectionTimeout();			
			Manager.setCurrentState(Manager.Follower);
			Manager.getCurrentState().voteRequested(msg);
			
		} else if (msg.getRequestVote().getTerm() <= Manager.getTerm())
			replyVote(msg, false);
	}

	@Override
	public synchronized void replyVote(WorkMessage msg, boolean voteGranted) {
		int toNodeId = msg.getHeader().getNodeId();
		int fromNodeId = Manager.getNodeId();
		EdgeInfo ei = Manager.getEmon().getOutBoundList().getEdgeMap().get(toNodeId);

		if (ei.isActive() && ei.getChannel()!=null) {
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
			this.votedFor = -1;
			Manager.setTerm((int) msg.getAppendMessage().getTerm());
			Manager.setCurrentState(Manager.Follower);
			Manager.randomizeElectionTimeout();
			System.out.println("Leader Elected. Leader is : " + msg.getAppendMessage().getLeaderId());
			Manager.getCurrentState().getAppendMessage(msg);
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
