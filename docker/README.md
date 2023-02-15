# Testing docker container

Container serve as a testing server with preinstalled and preconfigured SSH daemon.

- Username: bob
- Private key: ./priv


### Setup
```
docker build -t backup_test .
docker run -p 2022:22 -t -d backup_test
```

### Connect
```
ssh bob@127.0.0.1 -p 2022 -i priv
```
