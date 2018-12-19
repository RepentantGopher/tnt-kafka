NETWORK="tnt-kafka-tests"

docker-remove-network:
	docker network remove ${NETWORK} || true

docker-create-network: docker-remove-network
	docker network create ${NETWORK}

docker-remove-zoo:
	docker rm -f zookeeper || true

docker-run-zoo: docker-remove-zoo
	docker run -d \
        --net=${NETWORK} \
        --name=zookeeper \
        -p 2181:2181 \
        -e ZOOKEEPER_CLIENT_PORT=2181 \
        confluentinc/cp-zookeeper:5.0.0

docker-remove-kafka:
	docker rm -f kafka || true

docker-run-kafka: docker-remove-kafka
	docker run -d \
        --net=${NETWORK} \
        --name=kafka \
        -p 9092:9092 \
        -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        confluentinc/cp-kafka:5.0.0

docker-create-test-topic:
	docker run \
		--net=${NETWORK} \
		--rm confluentinc/cp-kafka:5.0.0 \
		kafka-topics --create --topic test_producer --partitions 1 --replication-factor 1 \
		--if-not-exists --zookeeper zookeeper:2181

docker-read-topic-data:
	docker run \
      --net=${NETWORK} \
      --rm \
      confluentinc/cp-kafka:5.0.0 \
      kafka-console-consumer --bootstrap-server kafka:9092 --topic test_producer --from-beginning

APP_NAME = kafka-test
APP_IMAGE = kafka-test-image

docker-build-app:
	docker build -t ${APP_IMAGE} -f ./docker/Dockerfile .

docker-remove-app:
	docker rm -f ${APP_NAME} || true

docker-run-app: docker-build-app docker-remove-app
	docker run -d \
		--net ${NETWORK} \
		--name ${APP_NAME} \
		-e KAFKA_BROKERS=kafka:9092 \
		${APP_IMAGE}

docker-run-interactive: docker-build-app docker-remove-app
	docker run -it \
		--net ${NETWORK} \
		--name ${APP_NAME} \
		-e KAFKA_BROKERS=kafka:9092 \
		${APP_IMAGE}

docker-remove-all: \
	docker-remove-app \
	docker-remove-kafka \
	docker-remove-zoo \
	docker-remove-network

docker-run-all: \
	docker-remove-all \
	docker-create-network \
	docker-run-zoo \
	docker-run-kafka \
	docker-build-app \
	docker-create-test-topic \
	docker-run-app
