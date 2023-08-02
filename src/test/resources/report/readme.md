# build image
```sh
cp  ../../../../build/libs/*.jar  files/
docker build  -t apache/flink-table-walkthrough:1-FLINK-1.12-scala_2.12 .   
```

# run env
dpcker-compose up