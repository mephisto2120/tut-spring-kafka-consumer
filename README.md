[![CircleCI](https://dl.circleci.com/status-badge/img/gh/mephisto2120/tut-spring-kafka-consumer/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/mephisto2120/tut-spring-kafka-consumer/tree/master)
# tut-spring-kafka-consumer-consumer

Installing it on amazon EC2 with Amazon Linux 2:
1. Install docker: https://gist.github.com/npearce/6f3c7826c7499587f00957fee62f8ee9Hi

docker run -p 9020:9020 --env ACTIVE_PROFILES=prod --env KAFKA_BOOTSTRAP_SERVERS=172.31.35.102:9092 --env openSearchClientUrl=http://172.31.33.34:9200 -d mephisto2120/tut-spring-kafka-consumer:latest
