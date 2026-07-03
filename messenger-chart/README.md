## Quick Start

```bash
# 1. Start minikube
minikube start --cpus=4 --memory=8g

# 2. Enable ingress (to avoid port-forward)
minikube addons enable ingress

# 3. Point Docker to minikube
eval $(minikube docker-env)
export DOCKER_BUILDKIT=1

# 4. Build images
# 4.1 Start SBT shell
sbt
# 4.2 Build all three services at once
sbt:messenger-cluster-system> pubAll

# 4.3 Build a single service
sbt:messenger-cluster-system> gateway/docker:publishLocal
sbt:messenger-cluster-system> auth/docker:publishLocal

# 5. Install the chart (dev mode)
helm install messenger ./messenger-chart -f ./messenger-chart/values-dev.yaml

# 6. Add host entry (one time)
echo "$(minikube ip) messenger.local" | sudo tee -a /etc/hosts

# 7. Wait for readiness
kubectl get pods -w
# Wait until all pods show Running + 1/1

# 8. Open in browser
open http://messenger.local
```

## Development Cycle

Change code → rebuild image → roll the pod:

```bash
eval $(minikube docker-env)

# Rebuild all services
sbt "gateway / docker:publishLocal" "auth / docker:publishLocal" "chat_core / Docker / publishLocal"

# Rebuild a single service
sbt "gateway / docker:publishLocal"   # gateway
sbt "auth / docker:publishLocal"      # auth
sbt "chat_core / Docker / publishLocal"  # core

# Roll a specific pod (zero-downtime)
kubectl rollout restart deployment/messenger-api-gateway
kubectl rollout restart deployment/messenger-auth
kubectl rollout restart deployment/messenger-chat-core

# Wait for rollout
kubectl get pods -w
```

### If shared protocols changed (EntityProtocol/*.java)

Shared classes are bundled into all three services, so all must be rebuilt:

```bash
eval $(minikube docker-env) && \
  sbt "gateway / docker:publishLocal" "auth / docker:publishLocal" "chat_core / Docker / publishLocal" && \
  kubectl rollout restart deployment/messenger-api-gateway && \
  kubectl rollout restart deployment/messenger-auth && \
  kubectl rollout restart deployment/messenger-chat-core && \
  kubectl get pods -w
```

### If Cassandra schema changed

Edit the CQL manifest in `cassandra/init.cql` and recreate the Cassandra pod:

```bash
kubectl delete pod cassandra-0
kubectl get pods -w
```

---

## Testing

Run integration tests after deployment:

```bash
# Ensure all pods are Ready
kubectl get pods

# Integration tests (full cycle: register, server, channel, message, read state)
bash test/integration_test.sh

# Complex tests (cross-user WebSocket push events)
bash test/complex_test.sh
```

Both scripts return **0** on success and print `ALL TESTS PASSED` or `ALL COMPLEX TESTS PASSED`.

Chart config changed → update the release:

```bash
helm upgrade messenger ./messenger-chart -f ./messenger-chart/values-dev.yaml
```

## One-liner Commands

```bash
# Enable ingress (one time)
minikube addons enable ingress

# Build everything
eval $(minikube docker-env) && sbt "gateway / docker:publishLocal" "auth / docker:publishLocal" "core / docker:publishLocal"

# Fresh install
helm install messenger ./messenger-chart -f ./messenger-chart/values-dev.yaml

# Update
helm upgrade messenger ./messenger-chart -f ./messenger-chart/values-dev.yaml

# Uninstall
helm uninstall messenger

# Stop minikube
minikube stop

# Destroy minikube (if everything is broken)
minikube delete
```

## Logs and Debugging

```bash
# Stream logs from all cluster pods
stern messenger --tail 50

# Logs for a specific service
kubectl logs -l role=gateway -f

# Previous pod logs (if the pod crashed)
kubectl logs -l role=gateway --previous

# Pod status
kubectl get pods -w

# Pod details (events, reasons)
kubectl describe pod -l role=gateway
```