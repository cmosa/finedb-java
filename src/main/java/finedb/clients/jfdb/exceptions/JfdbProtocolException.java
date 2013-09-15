package finedb.clients.jfdb.exceptions;

public class JfdbProtocolException extends RuntimeException {

	private static final long serialVersionUID = -6410250711393033752L;

	public JfdbProtocolException(String message){
		super(message);
	}
	
	public JfdbProtocolException(Throwable e){
		super(e);
	}
	
	public JfdbProtocolException(String message, Throwable e){
		super(message, e);
	}
	
}
