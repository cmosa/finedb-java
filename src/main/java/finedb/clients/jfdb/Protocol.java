package finedb.clients.jfdb;

public class Protocol {
	
	
	
	public static enum Command {
		PING ((byte)0x0)		// 0000
		, GET ((byte)0x1)		// 0001
		, DEL ((byte)0x2)		// 0010
		, PUT ((byte)0x3)		// 0011
		, SETDB ((byte)0x4)		// 0100
		, START ((byte)0x5)		// 0101
		, STOP ((byte)0x6)		// 0110
		, ADMIN ((byte)0xe)		// 1110
		, EXTRA ((byte)0xf);	// 1111
		 
		private final byte command;
		
		Command(byte command){
			this.command = command;
		}
		
		byte byteValue(){
			return command;
		}
	}

}
