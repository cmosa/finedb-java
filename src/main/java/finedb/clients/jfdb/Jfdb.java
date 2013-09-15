package finedb.clients.jfdb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

import finedb.clients.jfdb.exceptions.JfdbConnectionException;
import finedb.clients.jfdb.exceptions.JfdbProtocolException;

/**
 * FineDB Java Client
 * @author cmosa
 *
 */
public class Jfdb {
	
	public static final byte isSync = 1 << 4;
	public static final byte isSerialized = 1 << 5;
	public static final byte isCompressed = 1 << 6;
	public static final byte isServerCommand = (byte) (1 << 7);
	public static final int maxKeyLength = 0xFFFF; // 64K 

	private String host;
	private int port;
	private Socket socket;
	private DataOutputStream out;
	private DataInputStream in;
	
	public Jfdb(final String host, final int port){
		this.host = host;
		this.port = port;
	}
	
	/**
	 * Put data (uncompressed) to FineDB
	 * @param key
	 * @param data
	 * @return true if successful
	 */
	public boolean put(String key, byte[] data){
		connect();
		
		byte request = Protocol.Command.PUT.byteValue();
		request |= isSync;
		
		byte[] keyBytes = getKeyBytes(key);
		if(keyBytes.length > maxKeyLength){
			throw new JfdbProtocolException("Key length is maximum 64KB");
		}
		
		try {
			writeRequest(request, keyBytes, data);
		} catch (IOException e){
			throw new JfdbConnectionException(e);
		}
		
		try {
			byte answer = in.readByte();
			if ((answer & 1) != 0) {
				return true;
			}
		} catch (IOException e) {
			throw new JfdbConnectionException(e);
		}
		
		return false;
		
	}
	
	/**
	 * Get data (uncompressed) from FineDB as a ByteBuffer
	 * @param key 
	 * @return
	 */
	public ByteBuffer get(String key){
		connect();
		
		byte request = Protocol.Command.GET.byteValue();
		request |= isSync;
		
		byte[] keyBytes = getKeyBytes(key);
		if(keyBytes.length > maxKeyLength){
			throw new JfdbProtocolException("Key length is maximum 64KB");
		}
		
		try {
			writeRequest(request, keyBytes);
		} catch (IOException e){
			throw new JfdbConnectionException(e);
		}
		
		return getAnswerData();
	}
	
	private ByteBuffer getAnswerData(){
		try {
			byte answer = in.readByte();
			if ((answer & 1) != 0) { // checking the last bit : protocol says 0 is error, 1 is good
				ByteBuffer answerLength = ByteBuffer.allocate(4);
				answerLength.put(in.readByte());
				answerLength.put(in.readByte());
				answerLength.put(in.readByte());
				answerLength.put(in.readByte());
				answerLength.flip();
				int dataLength = answerLength.asIntBuffer().get(0);
				ByteBuffer data = ByteBuffer.allocate(dataLength);
				for(int i = 0; i < dataLength; i++){
					data.put(in.readByte());
				}
				return data;
			} 
		} catch (IOException e){
			throw new JfdbConnectionException(e);
		}
		return null;
	}
	
	private void writeRequest(byte request, byte[] keyBytes, byte[] data) throws IOException{
		ByteBuffer keyLength = ByteBuffer.allocate(4);
		keyLength.putInt(keyBytes.length);
		ByteBuffer dataLength = ByteBuffer.allocate(4);
		dataLength.putInt(data.length);
		
		out.writeByte(request);
		out.writeByte(keyLength.array()[2]);
		out.writeByte(keyLength.array()[3]);
		for(byte b : keyBytes){
			out.writeByte(b);
		}
		
		out.writeByte(dataLength.array()[0]);
		out.writeByte(dataLength.array()[1]);
		out.writeByte(dataLength.array()[2]);
		out.writeByte(dataLength.array()[3]);
		for(byte b : data){
			out.writeByte(b);
		}
		
	}
	
	private void writeRequest(byte request, byte[] keyBytes) throws IOException{
		ByteBuffer keyLength = ByteBuffer.allocate(4);
		keyLength.putInt(keyBytes.length);
		out.writeByte(request);
		out.writeByte(keyLength.array()[2]);
		out.writeByte(keyLength.array()[3]);
		for(byte b : keyBytes){
			out.writeByte(b);
		}
	}
	
	private byte[] getKeyBytes(String key){
		byte[] keyBytes;
		try {
			keyBytes = key.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new JfdbConnectionException(e);
		}
		return keyBytes;
	}
	
    public boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed()
                && socket.isConnected() && !socket.isInputShutdown()
                && !socket.isOutputShutdown();
    }
    
    private void connect(){
    	if(!isConnected()){
			try {
			 socket = new Socket();
             socket.setReuseAddress(true);
             socket.setKeepAlive(true);  //Will monitor the TCP connection is valid
             socket.setTcpNoDelay(true);  //Socket buffer Whetherclosed, to ensure timely delivery of data
             socket.setSoLinger(true,0);  //Control calls close () method, the underlying socket is closed immediately

             socket.connect(new InetSocketAddress(host, port), 1000);
             socket.setSoTimeout(1000);
             
             out = new DataOutputStream(socket.getOutputStream());
             in = new DataInputStream(socket.getInputStream());
			} catch (IOException e){
				throw new JfdbConnectionException(e); 
			}
		}
    }

	

}
