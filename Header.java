/** Message header, showing the fields used */
public class Header {
	Type type;
	int SequenceNr;
	int MessageNr;
	int SenderID;
	int DestID;
	
	String headerToString (Header h) {
		String s = h.type.toString() + '&' + Integer.toString(h.SequenceNr) + '&'
				+ Integer.toString(h.MessageNr) + '&' + Integer.toString(h.SenderID) + '&' + Integer.toString(h.DestID);
		return s;
	}
}