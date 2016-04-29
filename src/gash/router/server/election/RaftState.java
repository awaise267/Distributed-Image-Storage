package gash.router.server.election;

import pipe.work.Work.WorkMessage;

public interface RaftState {
	public void setManager(RaftManager Mgr);

	public RaftManager getManager();

	public void process();

	public void receivedVote(WorkMessage msg);

	public void replyVote(WorkMessage msg, boolean voteGranted);

	public void voteRequested(WorkMessage msg);

	public void sendAppendMessage();

	public void getAppendMessage(WorkMessage msg);

	public void sendAppendReply(WorkMessage msg, boolean successStatus);

	public void getAppendReply(WorkMessage msg);
}
