docker-compose -f "C:\Dev\twitter-stream\docker-compose\common.yml" -f "C:\Dev\twitter-stream\docker-compose\kafka_cluster.yml" up -d
timeout /t 15
docker-compose -f "C:\Dev\twitter-stream\docker-compose\common.yml" -f "C:\Dev\twitter-stream\docker-compose\config-server.yml" up -d
timeout /t 15
docker-compose -f "C:\Dev\twitter-stream\docker-compose\common.yml" -f "C:\Dev\twitter-stream\docker-compose\services.yml" up