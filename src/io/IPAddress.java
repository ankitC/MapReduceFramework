package io;

import common.Pair;

import java.net.Inet4Address;
import java.net.UnknownHostException;


/* Helper class for addressing the hosts */
public class IPAddress implements Comparable<IPAddress> {

    private Pair<String, Integer> ipAddress;

    public IPAddress(String address, Integer port) {
        try {
            String ipAddress = Inet4Address.getByName(address).getHostAddress();
            this.ipAddress = new Pair<String, Integer>(ipAddress, port);
        } catch (UnknownHostException e) {
            System.err.println("Could not get address of hostname " + address);
            e.printStackTrace();
        }
    }

    public String getAddress() {
        return ipAddress.getX();
    }

    public Integer getPort() {
        return ipAddress.getY();
    }

    @Override
    public int compareTo(IPAddress o) {
        return ipAddress.compareTo(o.ipAddress);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IPAddress)) return false;

        IPAddress address = (IPAddress) o;

        if (ipAddress != null ? !ipAddress.equals(address.ipAddress) : address.ipAddress != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return ipAddress != null ? ipAddress.hashCode() : 0;
    }
}

