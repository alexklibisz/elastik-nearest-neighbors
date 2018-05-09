# !/bin/sh
# This is a simple script to setup Elasticsearch on an Ubuntu EC2 instance.
# You could likely get more fancy with Docker/Chef/Puppet etc.

set -e

clustername=$1
esdir="$HOME/ES624"
cnf="$esdir/config/elasticsearch.yml"

sudo sysctl -w vm.max_map_count=262144

rm -rf $esdir
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.2.4.tar.gz
tar xvf elasticsearch-6.2.4.tar.gz
rm elasticsearch-6.2.4.tar.gz
mv elasticsearch-6.2.4 $esdir

sudo apt-get update -y
sudo apt-get install -y default-jre

echo "" > $cnf
echo "cluster.name: $clustername" >> $cnf
echo "node.name: $(cat /etc/hostname)" >> $cnf
echo "path.data: $HOME/esdata" >> $cnf
echo "path.logs: $HOME/eslogs" >> $cnf
echo "network.host: 0.0.0.0" >> $cnf
# echo "discovery.zen.hosts_provider: ec2" >> $cnf
echo "action.destructive_requires_name: true" >> $cnf
echo "http.cors.enabled: true" >> $cnf
echo "http.cors.allow-origin: /(null)|(https?:\/\/localhost(:[0-9]+)?)/" >> $cnf

echo "discovery.zen.ping.unicast.hosts: [\"ip-172-31-93-93.ec2.internal\", \"ip-172-31-88-105.ec2.internal\", \"ip-172-31-84-21.ec2.internal\", \"ip-172-31-87-186.ec2.internal\"]" >> $cnf


bash $esdir/bin/elasticsearch-plugin install -b discovery-ec2

ls ~/.aws
echo "----"
cat $cnf
echo "----"
which java
java -version
echo "----"
echo "Done"
