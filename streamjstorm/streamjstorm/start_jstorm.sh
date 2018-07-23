export HADOOP_HOME=/usr/local/hadoop-2.6.3
export JAVA_HOME=/usr/java/jdk1.7.0_79/
export PYTHON_HOME=/usr/bin/python
export JSTORM_HOME=$2
export CONFPATH=$2/conf
cp -rf $2/conf/storm.yaml  $5/storm.yaml
cp -rf $2/conf/jstorm.log4j.properties  $5/jstorm.log4j.properties
mkdir $5/data
chmod 777 $5/storm.yaml
echo -e " storm.local.dir: "$5/data" \n" >> $5/storm.yaml
echo -e " $1.deamon.logview.port: $3" >> $5/storm.yaml
echo -e " nimbus.thrift.port: ${4}" >> $5/storm.yaml
echo -e " storm.zookeeper.port: ${8}" >> $5/storm.yaml
echo -e " storm.zookeeper.root: '${6}'" >> $5/storm.yaml

echo -e " storm.zookeeper.servers: " >> $5/storm.yaml

OLD_IFS="$IFS"
IFS=","
arr=($7)
IFS="$OLD_IFS"
for s in ${arr[@]}
do
    echo -e "     - \"${s}\" " >> $5/storm.yaml
done


#写supervisor端口列表
echo "$9"
echo -e " supervisor.slots.ports:" >> $5/storm.yaml
OLD_IFS="$IFS"
IFS=","
arr=($9)
IFS="$OLD_IFS"
for s in ${arr[@]}
do
    echo -e "     - ${s} " >> $5/storm.yaml
done


cat $5/storm.yaml

export PATH=$PYTHON_HOME/bin:$JSTORM_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH
JAVA_HOME=/usr/java/jdk1.7.0_79/ python $2/bin/jstorm $1 
