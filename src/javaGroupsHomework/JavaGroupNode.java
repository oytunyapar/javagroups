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
    private Address m_leader_address;
    private final int m_maximum_id_number;
    private final long m_heart_beat_control_in_milliseconds;
    private final long m_heart_beat_control_factor;
    private final long m_sleep_duration_in_milliseconds;;
    Map<Integer, Long> m_heart_beat_time_map;
    Map<Integer, Address> m_address_map;
    private final String m_cluster_name;
    private boolean m_continue;
    private final short m_hearbeat_flag;
    private Message m_heart_beat_message;
    private final short m_kill_flag;
    private Message m_kill_message;
    private byte m_hearbeat_message_buffer[];
    private byte m_kill_message_buffer[];
    private Address m_self_address;

    /*
     * @throws Exception
     */
    public JavaGroupNode(int Id) throws Exception {
        m_maximum_id_number = (int) Byte.MAX_VALUE;

        if (Math.abs(Id) > m_maximum_id_number) {
            throw new Exception("Invalid ID");
        }

        m_id = Math.abs(Id);
        m_leader_id = -1;
        m_cluster_name = "ExampleCluster";
        m_sleep_duration_in_milliseconds = 500;
        m_heart_beat_control_factor = 3;
        m_heart_beat_control_in_milliseconds = m_sleep_duration_in_milliseconds * m_heart_beat_control_factor;
        m_heart_beat_time_map = new HashMap<Integer, Long>();
        m_address_map = new HashMap<Integer, Address>();
        m_communication_channel = new JChannel("udp.xml");
        m_hearbeat_message_buffer = new byte[1];
        m_kill_message_buffer = new byte[1];
        m_continue = false;
        m_hearbeat_flag = 1;
        m_kill_flag = 2;
        m_heart_beat_message = new Message();
        m_kill_message = new Message();

        if (m_heart_beat_message != null && m_hearbeat_message_buffer != null) {
            m_hearbeat_message_buffer[0] = (byte) m_id;
            m_heart_beat_message.setFlag(m_hearbeat_flag);
            m_heart_beat_message.setBuffer(m_hearbeat_message_buffer);
        } else {
            throw new Exception("Message buffer problem.");
        }

        if (m_kill_message != null && m_kill_message_buffer != null) {
            m_kill_message_buffer[0] = (byte) m_id;
            m_kill_message.setFlag(m_kill_flag);
            m_kill_message.setBuffer(m_kill_message_buffer);
        } else {
            throw new Exception("Message buffer problem.");
        }
    }

    private void checkHeartBeatsOfNodesInCluster() {
        int highetst_id = -1;
        long current_time = System.currentTimeMillis();

        for (Map.Entry<Integer, Long> entry : m_heart_beat_time_map.entrySet()) {
            if (current_time - entry.getValue() < m_heart_beat_control_in_milliseconds) {
                if (highetst_id < entry.getKey()) {
                    highetst_id = entry.getKey();
                }
            }
        }

        if (highetst_id < m_id) {
            if (m_id != m_leader_id) {
                m_leader_id = m_id;
                m_leader_address = m_self_address;
                System.out.println("I am the leader ID: " + m_id + " " + m_self_address);
            }
        } else {
            if (highetst_id != m_leader_id) {
                m_leader_id = highetst_id;
                m_leader_address = this.m_address_map.get(m_leader_id);
                System.out.println("Reporting node:" + m_id + " " + m_self_address + " the new leader ID: "
                        + m_leader_id + " leader address:" + m_leader_address.toString());
            }
        }
    }

    /*
     * @throws Exception
     */
    public boolean connect() throws Exception {

        if (null == m_communication_channel) {
            System.out.println("Connect::Communication channel is not initialized.");
            return false;
        }

        if (true == m_communication_channel.isConnected() || true == m_communication_channel.isConnecting()) {
            System.out.println("Connect::Already connected.");
        } else {
            m_communication_channel.setReceiver(this);
            m_communication_channel.connect(m_cluster_name);
        }

        return true;
    }

    public boolean disconnect() {
        if (null == m_communication_channel) {
            System.out.println("Disconnect::Communication channel is not initialized.");
            return false;
        }

        if (false == m_communication_channel.isConnected() && false == m_communication_channel.isConnecting()) {
            System.out.println("Disconnect::Already disconnected.");
        } else {
            System.out.println("Disconnect::disconnected. ID: " + m_id + " Address:"
                    + m_communication_channel.getAddressAsString());
            m_communication_channel.disconnect();
            m_communication_channel = null;
        }

        return true;
    }

    /*
     * @throws Exception
     */
    public void mainFunction() throws Exception {
        while (m_continue == false) {
            if (connect()) {
                m_continue = true;
            }
            else {
                throw new Exception("Connection request failed");
            }

            Thread.sleep(m_sleep_duration_in_milliseconds + m_sleep_duration_in_milliseconds / 2);
        }

        m_self_address = m_communication_channel.getAddress();

        while (m_continue) {
            if (true == m_communication_channel.isConnected()) {
                sendHeartbeat();
                checkHeartBeatsOfNodesInCluster();
            }

            Thread.sleep(m_sleep_duration_in_milliseconds);
        }
    }

    private void sendHeartbeat() throws Exception {
        if (m_heart_beat_message != null && m_communication_channel != null) {
            m_communication_channel.send(m_heart_beat_message);
        }
    }

    private void sendKill(Address address) throws Exception {
        if (m_kill_message != null && m_communication_channel != null) {
            if (m_id == m_leader_id && m_leader_address.toString() == m_self_address.toString()) {
                System.out.println("Kill message is sent by the leader: " + m_id + " to address: " + address.toString());
                m_communication_channel.send(address, m_kill_message);
            }
        }
    }

    @Override
    public void receive(Message msg) {
        int sender_id = -1;

        if (msg.getFlags() == m_hearbeat_flag) {
            sender_id = (int) msg.getBuffer()[0];
            Address sender_address = msg.getSrc();

            if (m_address_map.containsKey(sender_id)) {
                if (sender_address.toString() != m_address_map.get(sender_id).toString()) {
                    System.out.println(
                            "Duplicate node detected. ID: " + sender_id + " Address:" + sender_address.toString()
                                    + " Reporting Address:" + m_communication_channel.getAddressAsString());

                    try {
                        sendKill(sender_address);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return;
                }
            } else {
                m_address_map.put(sender_id, sender_address);
            }

            m_heart_beat_time_map.put(sender_id, System.currentTimeMillis());
        } else {
            Message unicast_message = null;
            if (msg.getObject() instanceof Message) {
                unicast_message = (Message) msg.getObject();
            }

            if (unicast_message != null) {
                sender_id = (int) unicast_message.getBuffer()[0];
                Address sender_address = msg.getSrc();

                if (unicast_message.getFlags() == m_kill_flag) {
                    System.out.println("Kill flag recevied. ID: " + m_id + " Sender ID:" + sender_id
                            + " Sender address:" + sender_address.toString());
                    if (sender_id == m_leader_id) {
                        if (m_leader_address != null && sender_address.toString() == m_leader_address.toString()) {
                            System.out.println("Goodbye cruel world. Leader killed me! ID:" + m_id + " Address: "
                                    + m_self_address.toString());
                            m_continue = false;
                        }
                    }
                } else {
                    System.out.println(
                            "Unknown unicast message received. Receiver Id:" + m_id + " Sender Id:" + sender_id);
                }
            } else {
                System.out.println("Unknown message received. Receiver Id:" + m_id + " Sender Id:" + sender_id);
            }
        }
    }

    @Override
    public void viewAccepted(View newView) {
        System.out.println("Cluster size is changed. Current cluster size is: " + newView.getMembers().size());
    }
}
