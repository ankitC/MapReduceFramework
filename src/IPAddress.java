public class IPAddress {

    private Pair<String, Integer> ipAddress;

    public IPAddress(String address, Integer port) {
        this.ipAddress = new Pair<String, Integer>(address, port);
    }

    public String getAddress() {
        return ipAddress.getX();
    }

    public Integer getPort() {
        return ipAddress.getY();
    }
}
