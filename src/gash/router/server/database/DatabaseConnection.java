package gash.router.server.database;

import io.netty.channel.Channel;
import pipe.common.Common.Header;
import routing.FileTransfer.FileMsg;
import routing.Pipe.CommandMessage;
import routing.Pipe.ResponseMsg;
import routing.Pipe.ResponseMsg.Operation;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.google.protobuf.ByteString;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import org.apache.commons.codec.binary.Base64;
import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DatabaseConnection {

	static Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

	static {
		root.setLevel(Level.OFF);
	}

	public static void sendFilesFromMongo(long fileId, Channel channel) {

		List<byte[]> filesFromDb = getFilesFromMongo(fileId);

		for (byte[] bytes : filesFromDb) {

			ResponseMsg.Builder rmb = ResponseMsg.newBuilder();

			FileMsg.Builder fmb = FileMsg.newBuilder();
			fmb.setChunkId(filesFromDb.indexOf(bytes));
			fmb.setFileChunk(ByteString.copyFrom(bytes));
			fmb.setFileId(fileId);
			fmb.setSize(filesFromDb.size());

			rmb.setFileId(fileId);
			rmb.setResponseStatus(true);
			rmb.setFileMsg(fmb);
			rmb.setOperation(Operation.Get);

			Header.Builder hb = Header.newBuilder();
			hb.setDestination(-1);
			hb.setNodeId(999);
			hb.setTime(System.currentTimeMillis());

			CommandMessage.Builder cb = CommandMessage.newBuilder();
			cb.setHeader(hb);
			cb.setReponseMsg(rmb);

			channel.writeAndFlush(cb.build());
		}

	}

	public static void sendFilesFromMySQL(long fileId, Channel channel) {

		List<byte[]> filesFromDb;
		try {
			filesFromDb = retrieveFromDatabase(fileId);

			for (byte[] bytes : filesFromDb) {

				ResponseMsg.Builder rmb = ResponseMsg.newBuilder();

				FileMsg.Builder fmb = FileMsg.newBuilder();
				fmb.setChunkId(filesFromDb.indexOf(bytes));
				fmb.setFileChunk(ByteString.copyFrom(bytes));
				fmb.setFileId(fileId);
				fmb.setSize(filesFromDb.size());

				rmb.setFileId(fileId);
				rmb.setResponseStatus(true);
				rmb.setFileMsg(fmb);
				rmb.setOperation(Operation.Get);

				Header.Builder hb = Header.newBuilder();
				hb.setDestination(-1);
				hb.setNodeId(999);
				hb.setTime(System.currentTimeMillis());

				CommandMessage.Builder cb = CommandMessage.newBuilder();
				cb.setHeader(hb);
				cb.setReponseMsg(rmb);

				channel.writeAndFlush(cb.build());

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static List<byte[]> getFilesFromMongo(long fileId) {
		MongoClient client = new MongoClient();
		System.setProperty("DEBUG.MONGO", "false");
		MongoCollection<Document> collection = client.getDatabase("FluffyFiles").getCollection("files");
		String fileName = "" + fileId;
		FindIterable<Document> iterable = collection.find(new Document("fileId", fileName));

		final List<byte[]> chunkList = new ArrayList<byte[]>();

		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				byte[] chunk = Base64.decodeBase64((String) document.get("data"));
				chunkList.add(chunk);
			}
		});

		client.close();
		return chunkList;
	}

	public static void putFilesToMongo(long fileId, int chunkId, byte[] bytes) {
		System.setProperty("DEBUG.MONGO", "false");
		String encodedBytes = Base64.encodeBase64String(bytes);
		MongoClient client = new MongoClient();
		MongoCollection<Document> collection = client.getDatabase("FluffyFiles").getCollection("files");
		String fileName = "" + fileId;
		String ChunkId = "" + chunkId;
		collection.insertOne(new Document("fileId", fileName).append("chunkId", ChunkId).append("data", encodedBytes));
		client.close();
		return;
	}

	public static void insertToDatabase(long fileId, int chunkId, byte[] bytes) throws Exception {
		try {

			Connection connect = null;
			PreparedStatement preparedStatement = null;

			// TODO Get connection string from config file.
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager
					.getConnection("jdbc:mysql://localhost:3306/fluffy?" + "user=root&password=xsw2XSW@");

			preparedStatement = connect.prepareStatement("insert into  fluffy.imagestore values (?,?,?)");

			preparedStatement.setLong(1, fileId);
			preparedStatement.setInt(2, chunkId);
			preparedStatement.setBytes(3, bytes);

			preparedStatement.executeUpdate();

		} catch (Exception e) {
			throw e;
		}
	}

	public static List<byte[]> retrieveFromDatabase(long fileId) throws Exception {
		try {
			List<byte[]> returnList = new ArrayList<byte[]>();
			Connection connect = null;
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;

			Class.forName("com.mysql.jdbc.Driver");
			// TODO Get connection string from the config file
			connect = DriverManager
					.getConnection("jdbc:mysql://localhost:3306/fluffy?" + "user=root&password=xsw2XSW@");

			preparedStatement = connect.prepareStatement("select * from  fluffy.imagestore where fileId = ?");

			preparedStatement.setLong(1, fileId);

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				// find column number for column containing the byte data and
				// get the bytes from that column
				returnList.add(resultSet.getBytes(resultSet.findColumn("bytedata")));
			}

			return returnList;

		} catch (Exception e) {
			throw e;
		}
	}

}
