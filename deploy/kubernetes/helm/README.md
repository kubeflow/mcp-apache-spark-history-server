# Helm Chart for MCP Server for Apache Spark History Server

This Helm chart provides a production-ready deployment of the MCP Server for Apache Spark History Server on Kubernetes.

## 🚀 Quick Start

### Prerequisites
- Kubernetes 1.30+
- Helm 3.8+
- Spark History Server running in cluster or accessible via network

### Install from Local Chart

```bash
# Install with default values
helm install mcp-apache-spark-history-server ./deploy/kubernetes/helm/mcp-apache-spark-history-server/

# Install with custom release name and namespace
helm install my-spark-mcp ./deploy/kubernetes/helm/mcp-apache-spark-history-server/ \
  --namespace spark-history-mcp \
  --create-namespace
```

### Install with Custom Values

```bash
# Create custom values file
cat > my-values.yaml << EOF
replicaCount: 3

config:
  servers:
    production:
      default: true
      url: "http://spark-history.production:18080"

auth:
  enabled: true
  secret:
    create: true
    username: "spark_user"
    password: "secure_password"

ingress:
  enabled: true
  hosts:
    - host: spark-mcp.company.com
      paths:
        - path: /
          pathType: Prefix

monitoring:
  enabled: true
  serviceMonitor:
    enabled: true

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
EOF

# Install with custom values
helm install my-spark-mcp ./deploy/kubernetes/helm/mcp-apache-spark-history-server/ -f my-values.yaml
```

## ⚙️ Configuration

### Common Configuration Examples

#### 1. Multiple Spark History Servers
```yaml
config:
  servers:
    production:
      default: true
      url: "http://prod-spark-history:18080"
    staging:
      url: "http://staging-spark-history:18080"
    development:
      url: "http://dev-spark-history:18080"
```

#### 1a. Dynamic EMR Clusters Configuration
```yaml
config:
  dynamic_emr_clusters_mode: true
```

#### 2. Authentication Setup
```yaml
auth:
  enabled: true
  secret:
    create: true
    username: "spark_admin"
    password: "super_secure_password"
    token: "jwt_token_here"
```

#### 3. High Availability Setup
```yaml
replicaCount: 3

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 20
  targetCPUUtilizationPercentage: 70

podDisruptionBudget:
  enabled: true
  minAvailable: 2

affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
        - key: app.kubernetes.io/name
          operator: In
          values:
          - mcp-apache-spark-history-server
      topologyKey: kubernetes.io/hostname
```

#### 4. Ingress with TLS
```yaml
ingress:
  enabled: true
  className: "nginx"
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/rate-limit-rps: "100"
  hosts:
    - host: spark-mcp.company.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: spark-mcp-tls
      hosts:
        - spark-mcp.company.com
```

#### 5. Monitoring and Observability
```yaml
monitoring:
  enabled: true
  serviceMonitor:
    enabled: true
    namespace: "monitoring"
    interval: 30s
    labels:
      release: prometheus

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "18888"
  prometheus.io/path: "/metrics"
```

#### 6. Resource Management
```yaml
resources:
  limits:
    memory: 4Gi
    cpu: 2000m
  requests:
    memory: 1Gi
    cpu: 500m

nodeSelector:
  kubernetes.io/arch: amd64
  node-type: compute

tolerations:
  - key: "spark-workload"
    operator: "Equal"
    value: "true"
    effect: "NoSchedule"
```

#### 7. Security Configuration
```yaml
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  seccompProfile:
    type: RuntimeDefault

securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  capabilities:
    drop:
    - ALL

networkPolicy:
  enabled: true
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
      - namespaceSelector:
          matchLabels:
            name: ai-agents
      ports:
      - protocol: TCP
        port: 18888
```

### Environment-Specific Values

#### Development Environment (`values-dev.yaml`)
```yaml
replicaCount: 1

config:
  debug: true
  servers:
    local:
      default: true
      url: "http://spark-history-dev:18080"

resources:
  limits:
    memory: 1Gi
    cpu: 500m
  requests:
    memory: 256Mi
    cpu: 100m

ingress:
  enabled: true
  hosts:
    - host: spark-mcp-dev.local
      paths:
        - path: /
          pathType: Prefix
```

#### Production Environment (`values-prod.yaml`)
```yaml
replicaCount: 5

config:
  debug: false
  servers:
    production:
      default: true
      url: "http://spark-history-prod:18080"

auth:
  enabled: true
  secret:
    create: false
    name: "spark-mcp-prod-auth"

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 20

monitoring:
  enabled: true
  serviceMonitor:
    enabled: true
    namespace: "monitoring"

networkPolicy:
  enabled: true

podDisruptionBudget:
  enabled: true
  minAvailable: 2
```

## 📊 Monitoring

### Prometheus Integration

When monitoring is enabled, the chart creates:
- ServiceMonitor for Prometheus scraping
- Grafana dashboard ConfigMaps
- Alert rules for common issues

```bash
# Install with monitoring
helm install my-spark-mcp ./mcp-apache-spark-history-server/ \
  --set monitoring.enabled=true \
  --set monitoring.serviceMonitor.enabled=true
```

### Custom Metrics

Add custom environment variables for metrics:

```yaml
env:
  - name: ENABLE_CUSTOM_METRICS
    value: "true"
  - name: METRICS_INTERVAL
    value: "30"
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Pod Not Starting
```bash
# Check pod status
kubectl describe pod -l app.kubernetes.io/name=mcp-apache-spark-history-server

# Check logs
kubectl logs -l app.kubernetes.io/name=mcp-apache-spark-history-server
```

#### 2. Configuration Issues
```bash
# Check rendered templates
helm template my-spark-mcp ./mcp-apache-spark-history-server/ -f my-values.yaml

# Verify ConfigMap
kubectl get configmap -l app.kubernetes.io/name=mcp-apache-spark-history-server -o yaml
```

#### 3. Connectivity Issues
```bash
# Test service connectivity
kubectl run test-pod --rm -i --tty --image=curlimages/curl -- sh
curl http://mcp-apache-spark-history-server:18888/health
```

### Debug Mode

Enable debug mode for troubleshooting:

```yaml
config:
  debug: true

# Add debug sidecar
sidecars:
  - name: debug
    image: busybox
    command: ["sleep", "3600"]
```

## 🔄 Upgrades

### Upgrade Chart

```bash
# Upgrade to new version
helm upgrade my-spark-mcp ./mcp-apache-spark-history-server/ -f my-values.yaml

# Rollback if needed
helm rollback my-spark-mcp 1
```

### Migration Guide

When upgrading from v0.0.x to v0.1.x:

1. **Backup configuration**:
```bash
kubectl get configmap -l app.kubernetes.io/name=mcp-apache-spark-history-server -o yaml > backup-config.yaml
```

2. **Update values file** according to new schema
3. **Perform rolling upgrade**:
```bash
helm upgrade my-spark-mcp ./mcp-apache-spark-history-server/ -f updated-values.yaml
```

## 🧪 Testing

### Validate Installation

```bash
# Run Helm tests
helm test my-spark-mcp

# Manual validation
kubectl run test-mcp --rm -i --tty --image=curlimages/curl -- sh
curl -X POST http://mcp-apache-spark-history-server:18888/tools \
  -H "Content-Type: application/json" \
  -d '{"tool": "list_applications", "parameters": {}}'
```

### Load Testing

```yaml
# Add load testing job
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-mcp-load-test
spec:
  template:
    spec:
      containers:
      - name: load-test
        image: loadimpact/k6
        command: ["k6", "run", "/scripts/load-test.js"]
        volumeMounts:
        - name: scripts
          mountPath: /scripts
      volumes:
      - name: scripts
        configMap:
          name: load-test-scripts
```

## 📚 Additional Resources

- [Kubernetes Documentation](../README.md) - Detailed K8s deployment guide
- [Values Reference](values.yaml) - Complete values documentation
- [Templates](templates/) - Kubernetes manifest templates
- [Examples](examples/) - Real-world configuration examples

## 🤝 Contributing

Contributions to the Helm chart are welcome:

- Chart improvements and new features
- Documentation updates
- Testing enhancements
- Bug fixes

See the main project [Contributing Guide](../../../README.md#-contributing) for details.
