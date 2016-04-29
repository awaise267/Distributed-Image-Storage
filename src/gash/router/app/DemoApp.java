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
package gash.router.app;

import java.io.IOException;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {

	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		//this.mc = mc;
		// this.mc.addListener(this);
	}

	private static void startProcess(MessageClient mc) throws IOException {
		Thread T = new Thread(mc);
		T.run();

	}

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		System.out.println("---> " + msg);
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String host = "127.0.0.1";
		int port = 4168;
		int nodeId = 1;

		MessageClient mc = new MessageClient(host, port, nodeId);
		//DemoApp da = new DemoApp(mc);

		// do stuff w/ the connection
		startProcess(mc);
		System.out.flush();
	}
}
