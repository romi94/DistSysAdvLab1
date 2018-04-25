import mcgui.*;

/**
 * Message implementation for ExampleCaster.
 *
 * @author Kruthika Suresh Ved, Romi Zaragatzky;
 */
public class ExampleMessage extends Message {
        
	Header header;  //The header
    String data;    //The data of the message
    String text;    //header + data (entire msg)
        
    public ExampleMessage(int sender,String d, Type type, int seqNr, int msgID) {
        super(sender);        
        header = new Header();
		header.type = type;
		header.SequenceNr = seqNr;
		header.MessageNr = msgID;
		header.SenderID = sender;
		if(d == null){  //If the transmitted data is null, make it empty string instead (to avoid compile errors)
			d = " ";
		}
		this.text = header.headerToString(header) + '&' + d;
		this.data = d;
    }
    
	public ExampleMessage(int sender, String d, Header h){
		super(sender);
		this.header = h;
		if(d == null){  //If the transmitted data is null, make it empty string instead (to avoid compile errors)
			d = " ";
		}
		this.text = this.header.headerToString(h) + '&' + d;
		this.data = d;		
	}
	
    public ExampleMessage(int sender,String text){
    	super(sender);
    	this.text = text;
    }
    /**
     * Returns the text of the message only. The toString method can
     * be implemented to show additional things useful for debugging
     * purposes.
     */
    public String getText() {
        return text;
    }
    
    /**
     * Dissolves the msg into header and data, assigns the header fields
     * their corresponding values.
     * @param text  The msg to be dissolved
     */
    public void dissolve(String text){
    	String[] arr = text.split("&", 6);  //The header fields are separated with '&'
    	this.header = new Header();
    	this.header.type = Type.valueOf(arr[0]);
    	this.header.SequenceNr = Integer.parseInt(arr[1]);
    	this.header.MessageNr = Integer.parseInt(arr[2]);
    	this.header.SenderID = Integer.parseInt(arr[3]);
    	this.header.DestID = Integer.parseInt(arr[4]);
    	if(arr.length > 5){ //If there exists any data
    		this.data = arr[5];
    	}
    	else{
    		this.data = " ";
    	}
    	this.text = text;
    }
    
    public static final long serialVersionUID = 0;
}
