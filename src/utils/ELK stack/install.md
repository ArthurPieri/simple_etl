# Install

- [Installation Guide from Elastic](https://www.elastic.co/guide/en/elastic-stack/current/installing-elastic-stack.html)

## Network requirements

To install the Elastic Stack on-premises, the following ports need to be open for each component.

| Default port | Component |
| ----- | ----- |
|3002 | Enterprise Search|
|5044 | Elastic Agent → Logstash|
|5044 | Beats → Logstash|
|5601 | Kibana|
|5601 | Elastic Agent → Fleet|
|5601 | Fleet Server → Fleet|
|8220 | Elastic Agent → Fleet Server|
|8220 | APM Server|
|9200-9300 | Elasticsearch REST API|
|9300-9400 | Elasticsearch node transport and communication|
|9600-9700 | Logstash REST API|

## Installation order

Install the Elastic Stack products you want to use in the following order:

- Elasticsearch ([install instructions](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/install-elasticsearch.html))
- Kibana ([install instructions](https://www.elastic.co/guide/en/kibana/8.11/install.html))
- Logstash ([install instructions](https://www.elastic.co/guide/en/logstash/8.11/installing-logstash.html))
- Elastic Agent ([install instructions](https://www.elastic.co/guide/en/fleet/8.11/elastic-agent-installation.html)) or Beats ([install instructions](https://www.elastic.co/guide/en/beats/libbeat/8.11/getting-started.html))
- APM ([install instructions](https://www.elastic.co/guide/en/apm/guide/8.11/apm-quick-start.html))
- Elasticsearch Hadoop ([install instructions](https://www.elastic.co/guide/en/elasticsearch/hadoop/8.11/install.html))

> Installing in this order ensures that the components each product depends on are in place.

## Install Elasticsearch using Docker

[Install guide](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/docker.html#_start_a_single_node_cluster)

### 1. Create a Network

```bash
docker network create elastic
```

### 2. Pull Image

```bash
docker pull docker.elastic.co/elasticsearch/elasticsearch:8.11.4
```

### 3. Start an elasticsearch container

```bash
docker run --name es01 --net elastic -p 9200:9200 -it -m 1GB docker.elastic.co/elasticsearch/elasticsearch:8.11.4
```

> The command prints the elastic user password and an enrollment token for Kibana.
> Copy the generated elastic password and enrollment token. These credentials are only shown when you start Elasticsearch for the first time. You can regenerate the credentials using the following commands.

```bash
docker exec -it es01 /usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic
docker exec -it es01 /usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s kibana
```

> It is recomended to set the password as an environment variable

```bash
    export ELASTIC_PASSWORD="your_password"
```

### 4. Copy the http_ca.crt SSL certificate from the container to your local machine

```bash
docker cp es01:/usr/share/elasticsearch/config/certs/http_ca.crt .
```

### 5. Test the connection

```bash
curl --cacert http_ca.crt -u elastic:$ELASTIC_PASSWORD https://localhost:9200
```

## Install Kibana using Docker

[Install Guide](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/docker.html#run-kibana-docker)

### 1. Pull Image

```bash
docker pull docker.elastic.co/kibana/kibana:8.11.4
```

### 2. Start kibana container

```bash
docker run --name kib01 --net elastic -p 5601:5601 docker.elastic.co/kibana/kibana:8.11.4
```

> When Kibana starts, it outputs a unique generated link to the terminal. To access Kibana, open this link in a web browser.
>
> In your browser, enter the enrollment token that was generated when you started Elasticsearch.

### 2.1 To regenerate the token

```bash
docker exec -it es01 /usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s kibana
```

### 2.2 To regenerate the password

```bash
docker exec -it es01 /usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic
```

## Install logstash using Docker

```bash
docker pull docker.elastic.co/logstash/logstash:8.11.4
```

Run and condigure logstash

```bash
docker run --rm -it -v ~/pipeline/:/usr/share/logstash/pipeline/ docker.elastic.co/logstash/logstash:8.11.4
```

## Install Elastic Agent

There are two images for Elastic Agent, elastic-agent and elastic-agent-complete. The elastic-agent image contains all the binaries for running Beats, while the elastic-agent-complete image contains these binaries plus additional dependencies to run browser monitors through Elastic Synthetics. Refer to Synthetic monitoring via Elastic Agent and Fleet for more information.

Run the docker pull command against the Elastic Docker registry:

```bash
docker pull docker.elastic.co/beats/elastic-agent:8.11.4
```

If you want to run Synthetics tests, run the docker pull command to fetch the elastic-agent-complete image:

```bash
docker pull docker.elastic.co/beats/elastic-agent-complete:8.11.4
```

### Get aware of the Elastic Agent Container command

```bash
docker run --rm docker.elastic.co/beats/elastic-agent:8.11.4 elastic-agent container -h
```

```bash
docker run \
  --env FLEET_SERVER_ENABLE=true \ 
  --env FLEET_SERVER_ELASTICSEARCH_HOST=<elasticsearch-host> \ 
  --env FLEET_SERVER_SERVICE_TOKEN=<service-token> \ 
  --env FLEET_SERVER_POLICY_ID=<fleet-server-policy> \ 
  -p 8220:8220 \ 
  --rm docker.elastic.co/beats/elastic-agent:8.11.4 
```

> [install heartbeat](https://www.elastic.co/guide/en/beats/heartbeat/8.11/heartbeat-installation-configuration.html#heartbeat-installation-configuration)
> [Metricbeat](https://www.elastic.co/guide/en/beats/metricbeat/8.11/metricbeat-overview.html)
> [APM Server](https://www.elastic.co/guide/en/apm/guide/8.11/apm-quick-start.html)
