
# 1. dashboard
kubectl get secret admin-user -n kubernetes-dashboard -o jsonpath="{.data.token}" | base64 -d

kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard-kong-proxy 8443:443


# 2. services
kubectl port-forward svc/kvs-admin-dashboard 9500:9500
