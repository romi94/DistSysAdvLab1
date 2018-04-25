import mcgui.*;
import java.util.*;
import java.lang.*;

/**
 * Simple example of how to use the Multicaster interface.
 *
 * @author Andreas Larsson &lt;larandr@chalmers.se&gt;
 */
public class ExampleCaster extends Multicaster{

	boolean isSeq;	//This is the sequencer
	int LastSeqReceived;	//The last sequence nr received by the receiver
	int MessageId;	//Next message id to be used by the sender
	int sequencerNr;	//The ID of the sequencer
	int[] SequenceStat;	//Array of the last received sequence nr from each sender
	Map<Integer, String> receiveBuffer;
	Map<Header, String> history;	//At the sequencer
	int MAX_SIZE = 10;	//Size of buffer
	int NextSeqtoUse;	//Next sequence nr that the sequencer will assign an incoming message
	boolean Phase1flag;	//When true, phase 1 of the synchronization phase has started
	int Phase1SeqStat;	//During the synchronization phase, the last seq nr received by a node (is sent to sequencer)
	boolean syncPhase;	//When true, the system is in the synchronization phase
	int ackCount;	//Count of the Acks received from nodes at the end of the synchronization phase
	LinkedList<ExampleMessage> SyncPhaseBuffer;	//Buffer at the sequencer that stores all incoming messages during sync phase
	boolean[] hostStat;	//The state of each node (true == up). Used when sequencer crashes
	LinkedList<ExampleMessage> senderBuffer;	//A buffer at the sender that stores sent msgs until corresponding success-msg received
	
    public void init() {
    	//Init at the start of the system
    	isSeq = false;
    	sequencerNr = 2;
    	MessageId = 1;		
		hostStat = new boolean[hosts];
		Arrays.fill(hostStat, true);
		senderBuffer = new LinkedList<ExampleMessage>();
        mcui.debug("The network has "+hosts+" hosts!");        
        initSeq();
    }
    
    private void initSeq(){
    	//Init when sequencer crashed
    	Phase1flag = false;
		LastSeqReceived = 0;
		receiveBuffer = new HashMap<Integer, String>();
    	if (id == sequencerNr) { //Code for sequencer
    		history = new HashMap<Header, String>();
        	isSeq = true;
    		SequenceStat = new int[hosts];
    		NextSeqtoUse = 0;
    		syncPhase = false;
    		ackCount = 1;
    		SyncPhaseBuffer = new LinkedList<ExampleMessage>();
    	}
    }
       
    /**
     * The GUI calls this module to to send msg to the sequencer (that will then broadcast it)
     * @param messagetext	The message that the sender wants to broadcast
     */
    public void cast(String messagetext)  {
    		ExampleMessage temp_msg = new ExampleMessage(id, messagetext, Type.DATA, LastSeqReceived, MessageId );
    		senderBuffer.add(temp_msg);	//Add to the buffer (removed when success-msg from sequencer received)
    		MyMessage msg = new MyMessage(id, temp_msg.text);
    		bcom.basicsend(sequencerNr, msg);	//Send msg to sequencer
    		mcui.debug("Sent msg: "+ messagetext);
    		MessageId++;	//For the next msg  
    }
    
    /**
     * Receive a message of any type (DATA,BROADCAST,RETRANS,PHASE1,PHASE2,ACK_COMMIT,SUCCESS,ACK_PHASE1)
     * and process it accordingly
     * @param peer	The peer that sent the message,
     * message  The message received
     */
    public void basicreceive(int peer, Message message) {
    	MyMessage my_msg = (MyMessage)message;
    	ExampleMessage rec_msg = new ExampleMessage(peer, my_msg.text);
    	rec_msg.dissolve(my_msg.text);	//Dissolve the message, to find the header fields
    	//Msgs received by the sequencer:
    	if (rec_msg.header.type == Type.DATA && !syncPhase) {	//Request to broadcast msg, should be broadcast immediately
    		processDataMsg(rec_msg);
    		return;
    	}
    	if (rec_msg.header.type == Type.DATA && syncPhase) {	//Request to broadcast msg, system in sync phase
    		SyncPhaseBuffer.add(rec_msg);	//save for later
    		return;
    	}
    	if(rec_msg.header.type == Type.SUCCESS) {	//Got success from sequencer == msg has been broadcast
    		for(ExampleMessage m : senderBuffer) {	//Remove the msg from the sender buffer
    			if (m.header.MessageNr == rec_msg.header.MessageNr) {
    				senderBuffer.remove(m);
    				break;
    			}
    		}
    		return;
    	}
    	if(rec_msg.header.type == Type.BROADCAST){  //Received broadcast from sequencer
    		if(rec_msg.header.SequenceNr == LastSeqReceived){   //It's the expected sequence nr
    			LastSeqReceived++;
    			mcui.deliver(rec_msg.header.SenderID, rec_msg.data );
				if(Phase1flag && Phase1SeqStat <= LastSeqReceived){ //During sync phase 1, received the last retransmit from sequencer
					ExampleMessage temp_msg = new ExampleMessage(id, "", Type.ACK_PHASE1, LastSeqReceived, MessageId);
					my_msg = new MyMessage(id, temp_msg.text);
					bcom.basicsend(sequencerNr, my_msg);    //Send acknowledgement to sequencer
				}
    		}
    		else{
    			ExampleMessage temp_msg = new ExampleMessage(id, "", Type.RETRANS, LastSeqReceived, MessageId);  //Ask sequencer to retransmit missed msg(s)
    			my_msg = new MyMessage(id, temp_msg.text);
    			bcom.basicsend(sequencerNr, my_msg);
    			receiveBuffer.put(rec_msg.header.SequenceNr, rec_msg.data );    //Put this msg in buffer in the meanwhile
    		}
            //Deliver all msgs from the buffer in the correct order
    		String temp;
    		while(receiveBuffer.get(LastSeqReceived) != null) {
    			temp = receiveBuffer.remove(LastSeqReceived);
    			mcui.deliver(peer, rec_msg.data);
    			LastSeqReceived++;
    		}
    		return;
    	}
    	if(rec_msg.header.type == Type.RETRANS) {   //Receiver missed a msg -> asks to retransmit
        	for (Header temp : history.keySet()) {
                //Find the msg with the corresponding sequence nr in history and retransmit it 
        		if ((temp.SequenceNr == rec_msg.header.SequenceNr)) {
        			temp.type = Type.BROADCAST;
        			ExampleMessage temp_msg = new ExampleMessage(temp.SenderID, history.get(temp), temp);  
        			my_msg = new MyMessage(temp.SenderID, temp_msg.text);
        			bcom.basicsend(sequencerNr, my_msg);
        			break;
        		}
        	}
        	return;
    	}
		if(rec_msg.header.type == Type.PHASE1){ //Sequencer started the sync phase 1
			Phase1flag = true;
			int i = LastSeqReceived;
			Phase1SeqStat = rec_msg.header.SequenceNr;
			if (Phase1SeqStat <= LastSeqReceived) { //This node is up-to-date, so just send ack
				ExampleMessage temp_msg = new ExampleMessage(id, "", Type.ACK_PHASE1, LastSeqReceived, MessageId);
				my_msg = new MyMessage(id, temp_msg.text);
				bcom.basicsend(sequencerNr, my_msg);
			}
			else {
                //Ask to sequencer to retransmit the missing msg(s)
				while(Phase1SeqStat > i) {
					ExampleMessage temp_msg = new ExampleMessage(id, "", Type.RETRANS, i, MessageId);  
					my_msg = new MyMessage(id, temp_msg.text);
    				bcom.basicsend(sequencerNr, my_msg);
					i++;
				}
			}
			return;
		}
		if(rec_msg.header.type == Type.PHASE2) {    //Sequencer started the sync phase 2
			Phase1flag = false; //Phase 1 finished
			ExampleMessage temp_msg = new ExampleMessage(id, "", Type.ACK_COMMIT, LastSeqReceived, MessageId);  //Send an ack to sequencer
			my_msg = new MyMessage(id, temp_msg.text);
			bcom.basicsend(sequencerNr, my_msg);
			return;
		}
		if(rec_msg.header.type == Type.ACK_COMMIT) {    //Received ack for sync phase 2 from node
			ackCount++; //Increase nr of acks
			if (ackCount == hosts) {    //All acked
				syncPhase = false;  //Sync phase finished
				ackCount = 1;   //Restart counter
                //Process all msgs that were received during the sync phase and clear the buffer
				LinkedList<ExampleMessage> tempBuffer = new LinkedList<ExampleMessage>();
				tempBuffer = (LinkedList<ExampleMessage>) SyncPhaseBuffer.clone();
				for(ExampleMessage s: tempBuffer){			
					processDataMsg(s);
				}
				SyncPhaseBuffer.clear();
			}
			return;
		}
		if(rec_msg.header.type == Type.ACK_PHASE1) {    //Received ack for sync phase 1 from node
			SequenceStat[rec_msg.header.SenderID] = rec_msg.header.SequenceNr;  //Update this node's last received sequence nr
			SequenceStat[id] = LastSeqReceived-1;   //Update my own (sequencer's) last received sequence nr
			if (minSeq() >= (NextSeqtoUse-1)) { //Everyone has received all broadcasted msgs
				history.clear();
                //Broadcast the phase 2 msg
				ExampleMessage temp_msg = new ExampleMessage(id, "", Type.PHASE2, NextSeqtoUse-1, MessageId); 
				my_msg = new MyMessage(id, temp_msg.text);
				for(int i=0; i < hosts; i++) {
					//Sends to everyone except itself
					if(i != id) {
						bcom.basicsend(i,my_msg);
					}
				}
			}
		}
	}
    
    /**
     * Process a data message
     * @param rec_msg	The received msg 
     */
    private void processDataMsg(ExampleMessage rec_msg) {
    		SequenceStat[rec_msg.header.SenderID] = rec_msg.header.SequenceNr; //The sequencer stores the piggybacked acknowledgement   
    		if(!fullHistory()){ 
    			rec_msg.header.SequenceNr = NextSeqtoUse; //Assign a sequence number
    			history.put(rec_msg.header, rec_msg.data); //Add the message in the history
    			LastSeqReceived++; //Updates the information for the 'receiver' part of the process 
    			mcui.deliver(rec_msg.header.SenderID, rec_msg.data );  
    			NextSeqtoUse++; //Keep the sequence number ready for next time
    			rec_msg.header.type = Type.BROADCAST; //Gearing up for broadcast
    			ExampleMessage temp_msg = new ExampleMessage(id, rec_msg.data, rec_msg.header); 
    			MyMessage my_msg = new MyMessage(id, temp_msg.text);
    			for(int i=0; i < hosts; i++) { //Broadcasting !!!
    				System.out.print("send to"+ i + "\n");
    				//Sends to everyone except itself
    				if(i != id) {
    					bcom.basicsend(i, my_msg);
    				}
    			}
    			rec_msg.header.type = Type.SUCCESS; //Send success message to the sender
    			temp_msg = new ExampleMessage(id, rec_msg.data, rec_msg.header);
    			my_msg = new MyMessage(id, temp_msg.text);
    			bcom.basicsend(rec_msg.header.SenderID, my_msg);
    		}
    		else{//The history is full...
    			SequenceStat[id] = LastSeqReceived;  //Updating my state
    			int min = minSeq(); //Find the messages which were received by everyone with the help of piggybacked acknowledgements 
    			Iterator<Map.Entry<Header,String>> iter = history.entrySet().iterator();
    			while (iter.hasNext()) {
    			    Map.Entry<Header,String> entry = iter.next();
    				if(entry.getKey().SequenceNr <= min){
    					iter.remove(); //remove the messages which are received by everyone
    				}    			
    			}
    			if(fullHistory()){ //History is still full 
    				SyncPhaseBuffer.add(rec_msg); //Sync phase is going to start,  so add the message in message in syncphase buffer 
    				EnterSyncPhase();
    			}
    			else{ //couldnt process this message as history was full.. now  we can complete the unfinished task
    				processDataMsg(rec_msg);
    			}
    		}
    }
	
    /**
     * Find the minimum sequence number in SequenceStat array
     */
    private int minSeq () {
    	int min;
		for(int i = 0; i<hosts; i++){
            if(i == 0){
                min = SequenceStat[i];
            }
			if(SequenceStat[i] < min){
				min = SequenceStat[i];
			}
		}
		return min;
    }
    
    /**
     * Check if history is full 
     */
    private boolean fullHistory () {    	
    	return (history.size() == MAX_SIZE);
    }
	
    /**
     * The phase1 of two phase commit protocol in order to clear the history (the synchromization phase)
     */
	private void EnterSyncPhase(){ 
		syncPhase = true; //the synchronization phase has started
		//Notifying everyone that the phase1 has started. And also requesting everyone to update themselves with the latest message.
		Header header = new Header(); 
		header.type = Type.PHASE1;
		header.SequenceNr = NextSeqtoUse-1;	
		header.SenderID = id;
		ExampleMessage temp_msg = new ExampleMessage(id, header.headerToString(header));
		MyMessage my_msg = new MyMessage(id, temp_msg.text);
		for(int i=0; i < hosts; i++) {
			//Sends to everyone except itself
			if(i != id) {
				bcom.basicsend(i,my_msg);
			}
		}
		//Notification done .. now everyone will send ACK_PHASE1 when they are up-to-date
	}
    
    /**
     * Signals that a peer is down and has been down for a while to
     * allow for messages taking different paths from this peer to
     * arrive.
     * @param peer	The dead peer
     */
    public void basicpeerdown(int peer) {    	
    	hostStat[peer] = false; //updating their status
        mcui.debug("Peer "+peer+" has been dead for a while now!"); 
    	if(peer == sequencerNr){ //Sequencer is down, we need to find new sequencer
    		for(int i = hosts -1; i >= 0 ; i--){ //Find the highest id who is still alive
    			if(hostStat[i]){
    				sequencerNr = i; //Sequencer found
    				break;
    			}
    		}
    		initSeq(); //intialize the sequencer objects 
    		for (ExampleMessage m : senderBuffer) { //Resend those messages to the new sequencer which were sent to the old sequencer but not yet broadcasted
    			MyMessage my_msg = new MyMessage(id, m.text);
    			bcom.basicsend(sequencerNr, my_msg);
    		} 
    	}        
    }
}