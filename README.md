# 📖 Key Value Store

A distributed, durable, in-memory key-value store built from scratch.

## Components

#### 1. Metadata Server
- Stores metadata (storage nodes, tables, partitions, partition leaders, configuration etc.)
- Uses RAFT for leader election
- Monitors storage server nodes
- Elects storage server partition leaders
#### 2. Storage Server
- Stores data in memory
- Uses leader-follower replication via gRPC or REST (configurable)
- Uses WAL and compressed snapshots for durability
- Supports TTL, secondary indexes, and cursor pagination
#### 3. Dashboard
- Provides full visibility into the cluster
- Supports admin operations like table creation
- Allows inserting mock data and performing basic load testing
#### 4. Client Library
- Lightweight library for client applications to interact with the key value store via REST
#### 5. Config Server
- Centralized configuration management service for all components
- Reads config data from another GitHub repo (config-repo)
#### 6. Consistent Hashing Library
- A lightweight consistent hashing implementation used by the client library to map item partition keys to partitions

#### Architectural Inspiration
> The replication architecture of the storage server is partly inspired by the Apache Kafka design.  
> This implementation is entirely independent and not affiliated with the Apache Kafka project.

---

## Running locally

#### Step 1 - Config Server
Since it's disabled locally, no need to start one. In k8s, it would be a dependency.
#### Step 2 - Metadata Server
- This one (only) does not run in Windows; if using Windows, run it in WSL. To start a single node with no followers, run:
<br> `sudo java -Dspring.profiles.active=local,local0 -jar target/kvs-metadata-server-0.0.1-SNAPSHOT.jar`
- To start a leader and two followers, run these:
<br> `sudo java -Dspring.profiles.active=local,local1 -jar target/kvs-metadata-server-0.0.1-SNAPSHOT.jar`
<br> `sudo java -Dspring.profiles.active=local,local2 -jar target/kvs-metadata-server-0.0.1-SNAPSHOT.jar`
<br> `sudo java -Dspring.profiles.active=local,local3 -jar target/kvs-metadata-server-0.0.1-SNAPSHOT.jar`
<br>In addition to the logs, the dashboard (below) shows which node is the current leader.
#### Step 3 - Storage Server
- It can be directly launched inside IntelliJ using the run configuration.
- Start any one (or more) of **Storage-local1**, **Storage-local2**, **and Storage-local3**.
#### Step 3 - Admin Dashboard
- It can be directly launched inside IntelliJ using the run configuration **KVS-AdminDashboard**
- Its default url is **localhost:9207**

---

## Deploying to kubernetes - EKS

- In the **deployment/kubernetes/scripts/images/tag-images.sh** script, replace the AWS account number
- In the **deployment/kubernetes/helm/services/api-gateway/templates/ingress.yaml** template, replace the domain
- run the **deploy_cluster.sh** script
- run `helm install <some name> config-server` under directory **deployment/kubernetes/helm/services**
- run `helm install <some name> env_dev` under directory **deployment/kubernetes/helm**
- Go to **https://kvsadmin.your-route53-domain.com**
<br>NOTE: it deploys other components as well for testing purpose, for example a mock online store application, which stores data in the key value store.

---

## 📄 License

MIT
