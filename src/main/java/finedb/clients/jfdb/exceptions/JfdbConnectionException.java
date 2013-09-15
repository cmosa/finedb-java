package finedb.clients.jfdb.exceptions;

public class JfdbConnectionException extends RuntimeException {

	private static final long serialVersionUID = -6410250711393033752L;

	public JfdbConnectionException(String message){
		super(message);
	}
	
	public JfdbConnectionException(Throwable e){
		super(e);
	}
	
	public JfdbConnectionException(String message, Throwable e){
		super(message, e);
	}
	
}
