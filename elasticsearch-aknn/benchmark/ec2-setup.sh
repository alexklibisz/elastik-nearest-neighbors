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
echo "action.destructive_requires_name: true" >> $cnf
echo "http.cors.enabled: true" >> $cnf
echo "http.cors.allow-origin: /(null)|(https?:\/\/localhost(:[0-9]+)?)/" >> $cnf

# Note: to get ec2 discovery working, either assign an IAM role with EC2 permissions
# to the instances running elasticsearch, or set the AWS_ACCESS_KEY_ID and 
# AWS_SECRET_ACCESS_KEY environment variables on the instance.
echo "discovery.zen.hosts_provider: ec2" >> $cnf
bash $esdir/bin/elasticsearch-plugin install -b discovery-ec2

ls ~/.aws
echo "----"
cat $cnf
echo "----"
which java
java -version
echo "----"
echo "Done"
