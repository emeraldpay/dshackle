package io.emeraldpay.dshackle.config;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.MethodProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertySubstitute;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

import javax.annotation.Nullable;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class Upstreams {

    private String version;
    private List<DefaultOptions> defaultOptions;
    private List<Upstream> upstreams;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<DefaultOptions> getDefaultOptions() {
        return defaultOptions;
    }

    public void setDefaultOptions(List<DefaultOptions> defaultOptions) {
        this.defaultOptions = defaultOptions;
    }

    public List<Upstream> getUpstreams() {
        return upstreams;
    }

    public void setUpstreams(List<Upstream> upstreams) {
        this.upstreams = upstreams;
    }

    public static class Options {
        private Boolean disableSyncing = true;
        private Integer minPeers = 1;
        private Integer quorum = 1;

        public Boolean getDisableSyncing() {
            return disableSyncing;
        }

        public void setDisableSyncing(Boolean disableSyncing) {
            this.disableSyncing = disableSyncing;
        }

        public Integer getMinPeers() {
            return minPeers;
        }

        public void setMinPeers(Integer minPeers) {
            this.minPeers = minPeers;
        }

        public Integer getQuorum() {
            return quorum;
        }

        public void setQuorum(Integer quorum) {
            this.quorum = quorum;
        }
    }

    public static class OptionsYaml extends TypeDescription {

        public OptionsYaml() {
            super(Options.class);
            super.substituteProperty(new PropertySubstitute("disable-syncing", Boolean.class,
                    "getDisableSyncing", "setDisableSyncing"));
            super.substituteProperty(new PropertySubstitute("min-peers", Integer.class,
                    "getMinPeers", "setMinPeers"));
        }
    }


    public static class DefaultOptions extends Options {
        private List<String> chains;
        private Options options;

        public List<String> getChains() {
            return chains;
        }

        public void setChains(List<String> chains) {
            this.chains = chains;
        }

        public Options getOptions() {
            return options;
        }

        public void setOptions(Options options) {
            this.options = options;
        }
    }

    public static class Upstream {
        private String id;
        private String chain;

        @Nullable
        private String provider;
        private List<Endpoint> endpoints = Collections.emptyList();
        private Options options;
        private boolean enabled = true;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getChain() {
            return chain;
        }

        public void setChain(String chain) {
            this.chain = chain;
        }

        @Nullable
        public String getProvider() {
            return provider;
        }

        public void setProvider(@Nullable String provider) {
            this.provider = provider;
        }

        public List<Endpoint> getEndpoints() {
            return endpoints;
        }

        public void setEndpoints(List<Endpoint> endpoints) {
            this.endpoints = endpoints;
        }

        public Options getOptions() {
            return options;
        }

        public void setOptions(Options options) {
            this.options = options;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Endpoint {
        private EndpointType type;
        private URI url;
        @Nullable
        private Auth auth;
        private Boolean enabled = true;
        @Nullable
        private URI origin;

        public EndpointType getType() {
            return type;
        }

        public void setType(EndpointType type) {
            this.type = type;
        }

        public URI getUrl() {
            return url;
        }

        public void setUrl(URI url) {
            this.url = url;
        }

        @Nullable
        public Auth getAuth() {
            return auth;
        }

        public void setAuth(@Nullable Auth auth) {
            this.auth = auth;
        }

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        @Nullable
        public URI getOrigin() {
            return origin;
        }

        public void setOrigin(@Nullable URI origin) {
            this.origin = origin;
        }
    }

    public static class Auth {
        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    public static class BasicAuth extends Auth {
        private String key;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

    public static class AuthYaml extends TypeDescription {

        public AuthYaml() {
            super(Auth.class);

        }

        @Override
        public Property getProperty(String name) {
            if ("key".equals(name)) {
                try {
                    return new MethodProperty(new PropertyDescriptor("key", BasicAuth.class, "getKey", "setKey"));
                } catch (IntrospectionException e) {
                    e.printStackTrace();
                }
            }
            return super.getProperty(name);
        }

        @Override
        public Object newInstance(Node node) {
            if (node instanceof MappingNode) {
                MappingNode mappingNode = (MappingNode)node;
                Optional<ScalarNode> type = mappingNode.getValue()
                        .stream()
                        .filter((n) -> n.getKeyNode() instanceof ScalarNode && n.getValueNode() instanceof ScalarNode)
                        .filter((n) -> {
                            ScalarNode sn = (ScalarNode)n.getKeyNode();
                            return "type".equals(sn.getValue());
                        })
                        .map((n) -> (ScalarNode)n.getValueNode())
                        .findFirst();
                if (type.isPresent()) {
                    if ("basic".equals(type.get().getValue())) {
                        return new BasicAuth();
                    } else {
                        throw new IllegalArgumentException("Unsupported auth type: " + type.get().getValue());
                    }
                } else {
                    throw new IllegalArgumentException("Auth type is not set");
                }
            }
            throw new IllegalArgumentException("Auth is invalid");
        }
    }

    public static enum EndpointType {
        JSON_RPC("json-rpc"),
        WEBSOCKET("ws", "websocket"),
        DSHACKLE("dshackle"),
        UNKNOWN("unknown");

        private final String[] code;

        EndpointType(String ... code) {
            this.code = code;
            Arrays.sort(this.code);
        }

        public static EndpointType byName(String code) {
            code = code.toLowerCase();
            for (EndpointType t: EndpointType.values()) {
                if (Arrays.binarySearch(t.code, code) >= 0) {
                    return t;
                }
            }
            return UNKNOWN;
        }
    }

    public static class EndpointTypeYaml extends TypeDescription {

        public EndpointTypeYaml() {
            super(EndpointType.class);
        }

        @Override
        public Object newInstance(Node node) {
            if (node instanceof ScalarNode) {
                return EndpointType.byName(((ScalarNode)node).getValue());
            }
            throw new IllegalArgumentException("Invalid type: " + node.getClass());
        }
    }
}