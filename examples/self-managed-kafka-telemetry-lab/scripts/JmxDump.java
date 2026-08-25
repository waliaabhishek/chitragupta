import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public final class JmxDump {
    private JmxDump() {}

    private static String jsonString(String value) {
        return "\"" + value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "\"";
    }

    private static boolean emitAttribute(MBeanServerConnection connection, ObjectName objectName, String attribute)
            throws IOException, MBeanException, AttributeNotFoundException, InstanceNotFoundException,
                    ReflectionException {
        Object value = connection.getAttribute(objectName, attribute);
        if (!(value instanceof Number) && !(value instanceof Boolean)) {
            return false;
        }
        System.out.printf(
                "{\"object_name\":%s,\"attribute\":%s,\"value\":%s}%n",
                jsonString(objectName.toString()),
                jsonString(attribute),
                value.toString());
        return true;
    }

    private static void dumpBrokerTopicMetrics(MBeanServerConnection connection) throws Exception {
        Set<ObjectName> names = connection.queryNames(
                new ObjectName("kafka.server:type=BrokerTopicMetrics,name=*,topic=*"), null);
        for (ObjectName name : names) {
            String metricName = name.getKeyProperty("name");
            if (metricName.equals("BytesInPerSec") || metricName.equals("BytesOutPerSec")) {
                emitAttribute(connection, name, "Count");
            }
        }
    }

    private static void dumpLogSize(MBeanServerConnection connection) throws Exception {
        Set<ObjectName> names = connection.queryNames(
                new ObjectName("kafka.log:type=Log,name=Size,topic=*,partition=*"), null);
        for (ObjectName name : names) {
            emitAttribute(connection, name, "Value");
        }
    }

    private static void dumpQuotas(MBeanServerConnection connection) throws Exception {
        for (String quotaType : new String[] {"Produce", "Fetch"}) {
            Set<ObjectName> names = connection.queryNames(new ObjectName("kafka.server:type=" + quotaType + ",*"), null);
            for (ObjectName name : names) {
                if (name.getKeyProperty("user") == null && name.getKeyProperty("client-id") == null) {
                    continue;
                }
                emitAttribute(connection, name, "byte-rate");
                emitAttribute(connection, name, "throttle-time");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: JmxDump <host> <port>");
        }
        String url = "service:jmx:rmi:///jndi/rmi://" + args[0] + ":" + args[1] + "/jmxrmi";
        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url))) {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            dumpBrokerTopicMetrics(connection);
            dumpLogSize(connection);
            dumpQuotas(connection);
        }
    }
}

