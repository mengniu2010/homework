paxos-image: ../cmd/* ../pkg/* Dockerfile
	docker build --tag paxos -f Dockerfile ..

svr: paxos-image docker-compose.yaml
	docker-compose up

cli: paxos-image
	docker run -it --rm --net build_paxos_net --name cli paxos /cmd/cli paxos-1:2222