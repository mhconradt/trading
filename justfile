trader_image := "565925249289.dkr.ecr.us-east-1.amazonaws.com/trader"
ingestion_image := "565925249289.dkr.ecr.us-east-1.amazonaws.com/trade-ingestion"

version := "1.7.15"


deploy_trader:
    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin {{ trader_image }}
    docker build -f dockerfiles/trader.Dockerfile -t {{ trader_image }}:{{ version }} .
    docker push {{ trader_image }}:{{version}}


deploy_ingestion:
    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin {{ ingestion_image }}
    docker build -f dockerfiles/ingestion.Dockerfile -t {{ ingestion_image }}:{{version}} .
    docker push {{ ingestion_image }}:{{version}}

watch:
    kubectl -n trading get pod | grep "Running" | grep "trader" | cut -c1-31 | xargs kubectl -n trading logs -f
