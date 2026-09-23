package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;

import java.util.HashMap;
import java.util.Map;

public class PersistentData {

    private Map<String, LeaseRecord> leases = new HashMap<>();
    private Map<String, LeaseResponse> responses = new HashMap<>();
    private Map<String, ResourceValue> resources = new HashMap<>();

    public Map<String, LeaseRecord> getLeases() { return leases; }
    public void setLeases(Map<String, LeaseRecord> leases) { this.leases = leases; }
    public Map<String, LeaseResponse> getResponses() { return responses; }
    public void setResponses(Map<String, LeaseResponse> responses) { this.responses = responses; }
    public Map<String, ResourceValue> getResources() { return resources; }
    public void setResources(Map<String, ResourceValue> resources) { this.resources = resources; }

    public static class ResourceValue {
        private String value;
        private long version;

        public ResourceValue() {
        }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public long getVersion() { return version; }
        public void setVersion(long version) { this.version = version; }
    }
}
