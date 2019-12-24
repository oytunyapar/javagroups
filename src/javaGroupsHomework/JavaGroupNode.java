package javaGroupsHomework;

import java.util.Map;
import java.util.HashMap;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;


public class JavaGroupNode extends ReceiverAdapter {
	
	private JChannel m_communication_channel;
	private int m_id;
	private int m_leader_id;
	private final long m_heart_beat_control_in_milliseconds;
	private final long m_heart_beat_control_factor;
	private final long m_sleep_duration_in_milliseconds;;
	Map<Integer,Long> m_heart_beat_time_map; 
	private final String m_cluster_name;
	private final short m_hearbeat_flag;
	private boolean m_continue;
	
	/*
	@throws Exception
	*/
	public JavaGroupNode(int Id) throws Exception
	{
		m_id = Math.abs(Id);
		m_leader_id = -1;
		m_cluster_name = "ExampleCluster";
		m_sleep_duration_in_milliseconds = 500;
		m_heart_beat_control_factor = 3;
		m_heart_beat_control_in_milliseconds = m_sleep_duration_in_milliseconds*m_heart_beat_control_factor;
		m_heart_beat_time_map = new HashMap<Integer,Long>(); 
		m_communication_channel = new JChannel("udp.xml");
		m_hearbeat_flag = 1;
		m_continue = true;
	}
	
	private void checkHeartBeatsOfNodesInCluster()
	{
		int highetst_id = -1;
		long current_time = System.currentTimeMillis();
		
        for (Map.Entry<Integer,Long> entry : m_heart_beat_time_map.entrySet())
        {
        	if ( current_time - entry.getValue() < m_heart_beat_control_in_milliseconds )
        	{
                if ( highetst_id < entry.getKey() )
                {
                	highetst_id = entry.getKey();
                }
        	}
        }
        
        if ( highetst_id < m_id  )
        {
        	if ( m_id != m_leader_id )
        	{
        	    m_leader_id = m_id;
        	    System.out.printf("I am the leader ID:%d \n", m_id);
        	}
        }
        else
        {
        	if ( highetst_id != m_leader_id )
        	{
        		m_leader_id = highetst_id;
        		System.out.printf("Reporting node: %d the new leader ID:%d \n", m_id, m_leader_id);
        	}
        }
	}
	
	/*
	@throws Exception
	*/
	public boolean connect() throws Exception
	{
		
		if ( null == m_communication_channel )
		{
			System.out.println("Connect::Communication channel is not initialized.");
			return false;
		}
		
		if ( true == m_communication_channel.isConnected() ||
			true == m_communication_channel.isConnecting() )
		{
			System.out.println("Connect::Already connected.");
		}
		else
		{
			m_communication_channel.setReceiver(this);
			m_communication_channel.connect(m_cluster_name);
		}
		
		return true;
	}
	
	public boolean disconnect()
	{
		if ( null == m_communication_channel )
		{
			System.out.println("Disconnect::Communication channel is not initialized.");
			return false;
		}
		
		if ( false == m_communication_channel.isConnected() &&
			 false == m_communication_channel.isConnecting() )
		{
			System.out.println("Connect::Already disconnected.");
		}
		else
		{
			m_communication_channel.disconnect();
		}
		
		return true;
	}
	
	/*
	@throws Exception
	*/
	public void mainFunction() throws InterruptedException
	{
		while(m_continue)
		{
			sendHeartbeat();
			checkHeartBeatsOfNodesInCluster();
			Thread.sleep(m_sleep_duration_in_milliseconds);
		}
	}
	
	private void sendHeartbeat()
	{
		
	}
	
	@Override
    public void receive(Message msg) {
        System.out.println("received message " + msg);
    }
	
    @Override
    public void viewAccepted(View newView) {
        //System.out.println("One more node joined. Current node size is: " + newView.getMembers().size());
    }

}
