#!/bin/bash

if [ "$#" = 0 ]; then
	printf "Usage:\n* To start the ZooKeeper servers: ./evalGM.sh prepare #-of-servers\n* To stop the ZooKeeper servers: ./evalGM.sh stop #-of-servers\n* To start clients: ./evalGM.sh start conf-file #-of-clients #-of-servers\n* To create groups: ./evalGM.sh create groups-file #-of-servers\n* To list existing groups: ./evalGM.sh list #-of-servers\n* To remove all group members: ./evalGM.sh close #-of-servers\n" 
else
if [ "$1" = "prepare" ] && [ "$#" = 2 ]; then
	for i in `seq 1 "$2"`
	do
	#creating directories and files, needed for starting the servers
		temp='zk'
		t=$temp$i
		port=`expr 2180 + "$i"`
		c1='zoo'
		c2='.cfg'
		config=$c1$i$c2
		mkdir /opt/zookeeper-3.4.9/data/"$t"
		echo "$i" > /opt/zookeeper-3.4.9/data/"$t"/myid
		printf "tickTime=2000\ninitLimit=5\nsyncLimit=2\ndataDir=/opt/zookeeper-3.4.9/data/"$t"\nclientPort="$port"\n" > /opt/zookeeper-3.4.9/conf/"$config"
		for j in `seq 1 "$2"`
		do
		t_p=`expr 1 + "$j"`
		t_p_1=`expr 1 + "$t_p"`
		printf "server."$j"=localhost:"$t_p""$t_p""$t_p""$t_p":"$t_p""$t_p""$t_p""$t_p_1"\n">> /opt/zookeeper-3.4.9/conf/"$config"
		done
		/opt/zookeeper-3.4.9/bin/zkServer.sh start /opt/zookeeper-3.4.9/conf/"$config"
	done
else
if [ "$1" = "stop" ] && [ "$#" = 2 ]; then
	for i in `seq 1 "$2"`
	do
		c1='zoo'
		c2='.cfg'
		config=$c1$i$c2
		/opt/zookeeper-3.4.9/bin/zkServer.sh stop /opt/zookeeper-3.4.9/conf/"$config"
	done
else
if [ "$1" = "start" ] && [ "$#" = 4 ]; then
	mvn -f /home/iva/workspace/GroupMember/pom.xml clean install -Dexec.args="start "$2" "$3" "$4""
else
if [ "$1" = "create" ] && [ "$#" = 3 ]; then
	mvn -f /home/iva/workspace/GroupMember/pom.xml clean install -Dexec.args="create "$2" "$3""
else
if [ "$1" = "list" ] && [ "$#" = 2 ]; then
	mvn -f /home/iva/workspace/GroupMember/pom.xml clean install -Dexec.args="list "$2""
else
if [ "$1" = "close" ] && [ "$#" = 2 ]; then
	mvn -f /home/iva/workspace/GroupMember/pom.xml clean install -Dexec.args="close "$2""
else
	printf "Usage:\n* To start the ZooKeeper servers: ./evalGM.sh prepare #-of-servers\n* To stop the ZooKeeper servers: ./evalGM.sh stop #-of-servers\n* To start clients: ./evalGM.sh start conf-file #-of-clients #-of-servers\n* To create groups: ./evalGM.sh create groups-file #-of-servers\n* To list existing groups: ./evalGM.sh list #-of-servers\n* To remove all group members: ./evalGM.sh close #-of-servers\n" 
fi
fi
fi
fi
fi
fi
fi
