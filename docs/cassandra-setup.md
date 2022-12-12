# Cassandra setup

## Project structure

```
.
├── data                     # cassandra database files
├── import
│    └── mav
│       └── train_data       # place data here to import
└── cassandra.yaml           # cassandra config file

```

## Setup

### Create the network

```ps
docker network create mav-cassandra-network
```

### Start the database

```ps
docker run --name mav-cassandra-db --network mav-cassandra-network -p 9042:9042 -v $PWD/import:/import -v $PWD/data:/var/lib/cassandra -v $PWD/cassandra.yaml:/etc/cassandra/cassandra.yaml -d cassandra:4.0.3
```

> Wait for the database to start up completely. It can take up to 20 seconds.

### Start CQLSH
```ps
docker run -it --network mav-cassandra-network --rm cassandra:4.0.3 cqlsh -u cassandra -p cassandra --request-timeout=3600 mav-cassandra-db
```

> If you get error like this: **Connection error: ('Unable to connect to any servers', {'172.23.0.2:9042': ConnectionRefusedError(111, "Tried connecting to [('172.23.0.2', 9042)]. Last error: Connection refused")})**, you can try it again later.

### Database setup

Run these commands in the CQLSH shell to create the database table.

1. Create the keyspace.
```sql
CREATE KEYSPACE IF NOT EXISTS mav WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
```

2. Create the table.

```sql
CREATE TABLE mav.train_data (epoch int,relation text,trainnumber text,created timeuuid,delay int,elviraid text,lat float,line text,lon float,servicename text,PRIMARY KEY (epoch, relation, trainnumber, created)) WITH CLUSTERING ORDER BY (relation ASC, trainnumber ASC, created ASC);
```

3. Create a new superuser.

```sql
CREATE USER {username} WITH PASSWORD '{password}' SUPERUSER;
```

4. Change the default user's password.

```sql
ALTER USER cassandra WITH PASSWORD 'dfsso67347mething54747long67a7ndincom4574prehensi562ble';
```

5. Now you can exit the CQLSH container.

```ps1
exit
```

### Restart the database

```ps
docker run --name mav-cassandra-db --network mav-cassandra-network -p 9042:9042 -v $PWD/import:/import -v $PWD/data:/var/lib/cassandra -v $PWD/cassandra.yaml:/etc/cassandra/cassandra.yaml -d cassandra:4.0.3
```

## Import data

### Unzip the files

Unzip the SSTable files, then move them to **/import/mav/train_data**

### Access the database container

```ps1
docker exec -it mav-cassandra-db bash
```
### Import the data with nodetool

```ps1
nodetool -h ::FFFF:127.0.0.1 import mav train_data /import/mav/train_data
```
