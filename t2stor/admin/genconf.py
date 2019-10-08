

ceph_conf = """
[global]

fsid = 149e7202-cac3-4181-bbb1-66fea2ca3be2
mon initial members = whx-ceph-1
mon host = 172.159.4.11
public network =  172.159.0.0/16
cluster network = 172.159.0.0/16
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
osd journal size = 1024
osd pool default size = 3
osd pool default min size = 2
osd pool default pg num = 333
osd pool default pgp num = 333
osd crush chooseleaf type = 1
"""

agent_conf = """
[DEFAULT]
debug=true
my_ip='{my_ip}'
api_port=2080
websocket_port=2081
admin_port=2082
agent_port=2083

[database]
connection = mysql+pymysql://stor:stor@192.168.211.129/stor
"""

yum_repo = """
[t2stor-base]
name=Base packages and requirements
baseurl=http://192.168.103.167:8080/repo/base
enabled=1
gpgcheck=0

[t2stor-ceph]
name=Ceph packages
baseurl=http://192.168.103.167:8080/repo/ceph-rpms
enabled=1
gpgcheck=0

[t2stor-tools]
name=Some useful tools
baseurl=http://192.168.103.167:8080/repo/tools
enabled=1
gpgcheck=0

[t2stor-elasticsearch]
name=Elasticsearch and java packages
baseurl=http://192.168.103.167:8080/repo/elasticsearch
enabled=1
gpgcheck=0

[t2stor-portal]
name=portal
baseurl=http://192.168.103.167:8080/repo/portal
enabled=1
gpgcheck=0
"""


def get_agent_conf(node_ip):
    return agent_conf.format(my_ip=node_ip)

