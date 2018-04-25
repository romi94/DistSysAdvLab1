import mcgui.*;

/**
* This class is used for sending and receiving purpose.
*
* @author Kruthika Suresh Ved, Romi Zaragatzky;
*/
public class MyMessage extends Message {
    
	String text;    //The msg
	public MyMessage(int sender,String text){
    	super(sender);
    	this.text = text;
    }
	public static final long serialVersionUID = 0;
}