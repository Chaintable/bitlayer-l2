# Docker Deployment Guide for Bitlayer-L2 Pipeline Tracer

This guide explains how to deploy bitlayer-l2 with pipeline tracer using Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- AWS credentials with S3 access
- Access to Kafka brokers

## Quick Start

```bash
# 1. Configure environment
cp .env.example .env
vim .env  # Edit with your configuration

# 2. Configure geth
cp config.toml.example config.toml
vim config.toml  # Adjust as needed

# 3. Start the node
docker-compose -f docker-compose.debank.yml up -d

# 4. View logs
docker-compose -f docker-compose.debank.yml logs -f
```

## Configuration

### 1. Environment Variables (.env)

```bash
# Data directory
DATA_DIR=/data/bitlayer-l2

# Config file path
CONFIG_FILE=./config.toml

# AWS S3 Configuration
NODE_X_BUCKET=chaintable-nodex-pipeline--apne1-az4--x-s3
CHAIN_TABLE_BUCKET=chaintable-pipeline--apne1-az4--x-s3
AWS_REGION=ap-northeast-1

# Kafka Configuration (JSON array format)
KAFKA_BROKERS=["b-1.kafka.example.com:9092","b-2.kafka.example.com:9092"]
KAFKA_TOPIC=nodex_pipeline_200901_latest

# Chain Configuration
CHAIN_ID=200901
VERSION=latest

# Backup node (true/false)
IS_BACKUP=false
```

### 2. Geth Configuration (config.toml)

See `config.toml.example` for a complete configuration template.

Key sections:
- `[Eth]` - Ethereum protocol settings
- `[Eth.Miner]` - Mining configuration
- `[Eth.TxPool]` - Transaction pool settings
- `[Node]` - P2P and RPC settings

### 3. Pipeline Tracer Configuration

The pipeline tracer is configured via the `--vmtrace.jsonconfig` flag in docker-compose.yml.
It uses environment variables from .env file:

```json
{
  "node_x_bucket": "${NODE_X_BUCKET}",
  "chain_table_bucket": "${CHAIN_TABLE_BUCKET}",
  "region": "${AWS_REGION}",
  "brokers": ${KAFKA_BROKERS},
  "topic": "${KAFKA_TOPIC}",
  "chain_id": "${CHAIN_ID}",
  "s3_temp_dir": "/var/data/s3_tmp",
  "is_backup": ${IS_BACKUP},
  "version": "${VERSION}"
}
```

## Docker Compose Structure

```yaml
version: '3.8'

networks:
  bitlayer-network:
    ipam:
      driver: default
      config:
        - subnet: 10.0.200.0/24

services:
  bitlayer-l2:
    image: 294354037686.dkr.ecr.ap-northeast-1.amazonaws.com/blockchain-bitlayer-l2-x:amd64-latest
    ports:
      - 8545:8545  # HTTP RPC
      - 8546:8546  # WebSocket RPC
      - 30303:30303  # P2P
    volumes:
      - ${DATA_DIR}:/var/data
      - ${CONFIG_FILE}:/etc/bitlayer-l2/config.toml
    entrypoint:
      - /usr/local/bin/geth
      - --config=/etc/bitlayer-l2/config.toml
      - --vmtrace=pipeline
      - --vmtrace.jsonconfig=${VMTRACE_CONFIG}
```

## Operations

### Start Node

```bash
docker-compose -f docker-compose.debank.yml up -d
```

### View Logs

```bash
# All logs
docker-compose -f docker-compose.debank.yml logs -f

# Last 100 lines
docker-compose -f docker-compose.debank.yml logs --tail=100 -f

# Pipeline tracer specific logs
docker-compose -f docker-compose.debank.yml logs -f | grep -i "pipeline\|OnBlock\|OnTx"
```

### Stop Node

```bash
# Graceful stop (60s grace period)
docker-compose -f docker-compose.debank.yml down

# Force stop
docker-compose -f docker-compose.debank.yml kill
```

### Restart Node

```bash
docker-compose -f docker-compose.debank.yml restart
```

### Access Container

```bash
docker exec -it bitlayer-l2-pipeline /bin/bash
```

### Test RPC

```bash
# Get block number
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  http://localhost:8545

# Get network ID
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"net_version","params":[],"id":1}' \
  http://localhost:8545
```

## Monitoring

### Check Pipeline Tracer Status

```bash
# Check if pipeline tracer is initialized
docker logs bitlayer-l2-pipeline 2>&1 | grep "vmtrace config"

# Check block processing
docker logs bitlayer-l2-pipeline 2>&1 | grep "OnBlockStart\|OnBlockEnd"

# Check S3 uploads
docker logs bitlayer-l2-pipeline 2>&1 | grep -i "s3\|upload"

# Check Kafka messages
docker logs bitlayer-l2-pipeline 2>&1 | grep -i "kafka\|publish"
```

### Check Metrics

```bash
# Prometheus metrics endpoint
curl http://localhost:9260/debug/metrics/prometheus
```

### Check pprof

```bash
# CPU profile
curl http://localhost:9260/debug/pprof/profile?seconds=30 > cpu.prof

# Heap profile
curl http://localhost:9260/debug/pprof/heap > heap.prof

# Goroutine profile
curl http://localhost:9260/debug/pprof/goroutine > goroutine.prof
```

## Troubleshooting

### Pipeline Tracer Not Initializing

1. Check configuration:
```bash
docker exec bitlayer-l2-pipeline printenv VMTRACE_CONFIG
```

2. Check logs for tracer errors:
```bash
docker logs bitlayer-l2-pipeline 2>&1 | grep -i "tracer\|pipeline" | head -50
```

3. Verify AWS credentials (if using IAM role, skip this):
```bash
# From host
aws s3 ls s3://${NODE_X_BUCKET}/

# From container (if AWS CLI installed)
docker exec bitlayer-l2-pipeline aws s3 ls s3://${NODE_X_BUCKET}/
```

### S3 Upload Failures

1. Check bucket permissions:
```bash
aws s3api get-bucket-acl --bucket ${NODE_X_BUCKET}
```

2. Test write access:
```bash
echo "test" | aws s3 cp - s3://${NODE_X_BUCKET}/test.txt
aws s3 rm s3://${NODE_X_BUCKET}/test.txt
```

3. Check container logs:
```bash
docker logs bitlayer-l2-pipeline 2>&1 | grep -i "s3.*error\|failed.*upload"
```

### Kafka Connection Issues

1. Test connectivity:
```bash
# Extract broker from KAFKA_BROKERS env
BROKER=$(echo $KAFKA_BROKERS | jq -r '.[0]' | cut -d: -f1)
PORT=$(echo $KAFKA_BROKERS | jq -r '.[0]' | cut -d: -f2)
telnet $BROKER $PORT
```

2. Check container network:
```bash
docker exec bitlayer-l2-pipeline nc -zv $BROKER $PORT
```

3. Verify topic exists:
```bash
# Using kafka-topics.sh (if available)
kafka-topics.sh --bootstrap-server $BROKER:$PORT --list | grep $KAFKA_TOPIC
```

### Node Not Syncing

1. Check peers:
```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}' \
  http://localhost:8545
```

2. Check sync status:
```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
  http://localhost:8545
```

3. Add static nodes (edit config.toml):
```toml
[Node.P2P]
StaticNodes = [
    "enode://...@ip:port",
    "enode://...@ip:port"
]
```

## Data Management

### Backup Blockchain Data

```bash
# Stop node
docker-compose -f docker-compose.debank.yml down

# Backup data directory
tar czf bitlayer-data-$(date +%Y%m%d).tar.gz ${DATA_DIR}

# Or use rsync for incremental backups
rsync -av ${DATA_DIR}/ /backup/bitlayer-l2/
```

### Clean Old Data

```bash
# Stop node
docker-compose -f docker-compose.debank.yml down

# Remove data directory
rm -rf ${DATA_DIR}

# Start fresh
docker-compose -f docker-compose.debank.yml up -d
```

### Prune State

Bitlayer-l2 is configured with `--gcmode=archive` to keep full state history.
To reduce disk usage, you can switch to `--gcmode=full` in docker-compose.yml.

## Production Deployment

### Recommended Settings

1. **Resource Limits**:
```yaml
services:
  bitlayer-l2:
    deploy:
      resources:
        limits:
          cpus: '8'
          memory: 32G
        reservations:
          cpus: '4'
          memory: 16G
```

2. **Health Check**:
```yaml
services:
  bitlayer-l2:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8545"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
```

3. **Logging**:
```yaml
services:
  bitlayer-l2:
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "10"
```

### High Availability

For production, consider:
- Multiple nodes with leader election (using etcd)
- Load balancer for RPC endpoints
- Separate archive and full nodes
- Regular backups
- Monitoring and alerting

### Security

1. **Firewall Rules**:
```bash
# Allow RPC only from specific IPs
iptables -A INPUT -p tcp --dport 8545 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 8545 -j DROP

# Allow P2P from anywhere
iptables -A INPUT -p tcp --dport 30303 -j ACCEPT
iptables -A INPUT -p udp --dport 30303 -j ACCEPT
```

2. **IAM Roles** (recommended over access keys):
   - Attach IAM role to EC2 instance
   - Remove AWS credentials from environment

3. **VPC Network**:
   - Run nodes in private subnet
   - Use NAT gateway for outbound traffic
   - Restrict security groups

## References

- [PIPELINE_MIGRATION.md](PIPELINE_MIGRATION.md) - Migration details and changes
- [Dockerfile.debank](Dockerfile.debank) - Docker image build
- [config.toml.example](config.toml.example) - Geth configuration template
- [.env.example](.env.example) - Environment variables template
