import boto3, uuid, os, argparse, sys, time, random, parameters
from types import FunctionType
"""
OVERVIEW:  This script does the following to deploy and configure a data generator
-- deploys an aws instance w/ security group
-- adds tags to instance
-- installs required linux packages (such as pip, git, etc)
-- attached EFS storage
-- downloads the data gen repo from github
-- configures it with a cron job to execute data generator code (writes files to /efs/dummydata)

EXAMPLE FOR parameter.py:
params = {
	,'region_name': 'us-west-2'
	,'config_key': 'linux003'
	,'KeyName': 'XXXXXXXXXXX'
	,'security_group_ids': ['XXXXXXXXXXX']
	,'pem_file': '~/XXXXXXXXXXX'
	,'user': 'ubuntu'
	,'efs': 'XXXXXXXXXXX.efs.us-west-2.amazonaws.com'
	}
configs = {
	 'linux003': {'ImageId':'ami-5323b533', 'InstanceType':'t2.micro', 'ImageType': 'Linux'} # Ubuntu 14.04
	}
"""
##############################################################################
# PARAMETERS - pull from the file parameters.py
##############################################################################
region_name = parameters.params['region_name']
config_key = parameters.params['config_key']
KeyName = parameters.params['KeyName']
security_group_ids = parameters.params['security_group_ids']
pem_file = parameters.params['pem_file']
user = parameters.params['user']
efs = parameters.params['efs']
configs = parameters.configs

##############################################################################
#start session (must have already initialized the AWS CLI security via "aws configure")
##############################################################################
session = boto3.Session(profile_name='default')
ec2_client = boto3.client('ec2',region_name=region_name)
ec2 = boto3.resource('ec2',region_name=region_name)

##############################################################################
# FUNCTIONS:
##############################################################################
def get_instance_state(InstanceId):
	instance_list = ec2.instances.filter(InstanceIds=[InstanceId])
	for instance in instance_list:
		out = instance.state['Name']
		break
	return out

def get_instance_list(status_group='all'):
	status_list = []
	if status_group=='all':
		status_list=['running','pending','shutting-down','terminated','stopping','stopped']
	if status_group=='run':
		status_list=['running','pending']
	if status_group=='stop':
		status_list=['shutting-down','terminated','stopping','stopped']
	return ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': status_list}])


def ec2_create_instances(
	instance_config
	, MinCount=1, MaxCount=1
	, AllocateNewAddress = False
	, image_type = 'default'
	, KeyName = KeyName
	, security_group_ids = security_group_ids
	, gb_list=[8,50]
	, DeviceName_list = None
	, volume_type = 'gp2'
	, Encrypted = False
	, DeleteOnTermination = True
	):
	if instance_config not in configs.keys():
		print 'instance configuration identifier is invalid.'
		return None
	else:
		print 'Creating instance with configuration: {instance_config}'.format(instance_config=instance_config)
	ImageId=configs[instance_config]['ImageId']
	InstanceType=configs[instance_config]['InstanceType']
	if image_type == 'default':
		image_type = configs[instance_config]['ImageType']
	response = ec2.create_instances(
		ImageId=ImageId
		, InstanceType=InstanceType
		, MinCount=MinCount, MaxCount=MaxCount
		, KeyName=KeyName
		, SecurityGroupIds=security_group_ids
		)

	print response
	tag = response[0].create_tags(Tags=[{'Key': 'image_type','Value': image_type},])
	return response

def ec2_instances_print(detail=0,status_group='all',instance_filter='all'):
	instances = get_instance_list(status_group)
	for instance in instances:
		if instance_filter == 'all' or instance_filter == instance.instance_id:
			if detail==0:
				print(instance.id, instance.instance_type, instance.state['Name'])
			if detail==1:
				print """
	ami_launch_index: {ami_launch_index}
	image_id: {image_id}
	instance_id: {instance_id}
	instance_type: {instance_type}
	launch_time: {launch_time}
	key_name: {key_name}
	key_pair: {key_pair}
	network_interfaces: {network_interfaces}
	placement: {placement}
	placement_group: {placement_group}
	platform: {platform}
	private_ip_address: {private_ip_address}
	public_ip_address: {public_ip_address}
	root_device_name: {root_device_name}
	root_device_type: {root_device_type}
	security_groups: {security_groups}
	state: {state}
	subnet: {subnet}
	tags: {tags}
	volumes: {volumes}
	vpc: {vpc}
	vpc_addresses: {vpc_addresses}
	vpc_id: {vpc_id}""".format(vpc_id=instance.vpc_id,vpc_addresses=instance.vpc_addresses
					,volumes=instance.volumes, vpc=instance.vpc, subnet=instance.subnet,image_id=instance.image_id
					,instance_id=instance.instance_id ,launch_time=instance.launch_time
					,key_name=instance.key_name,key_pair=instance.key_pair,network_interfaces=instance.network_interfaces
					,ami_launch_index=instance.ami_launch_index,public_ip_address=instance.public_ip_address
					,placement_group=instance.placement_group,platform=instance.platform,root_device_name=instance.root_device_name  # placement_group['name']
					,security_groups=instance.security_groups,state=instance.state['Name']
					,tags=instance.tags,instance_type=instance.instance_type
					,placement=instance.placement['AvailabilityZone'],private_ip_address=instance.private_ip_address,root_device_type=instance.root_device_type
					)


def create_shell_script(bash_file,id_Sales=None):
	if not id_Sales:
		#max value of 64 bit ints is 2^63, so approx 9*10^18
		# so this creates semi-random, semi-non-clashing ranges for each server
		#and stores it in an environmental variable for the ubuntu user in ".profile"
		#first get the ID range for the server:
		id_server = random.randint(1,9000000)
		id_Sales = id_server * 1000000000000
		print 'id_Sales: {id_Sales}'.format(id_Sales=id_Sales)
	cmd = '''
sudo apt-get install nfs-common -y
sudo mkdir /efs
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {efs}:/ /efs
sudo apt-get install git -y
sudo apt-get install python-pip -y
sudo pip install Faker
echo 'export id_Sales={id_Sales}' >> ~/.profile
source ~/.profile
#git clone https://github.com/ereimanclarity/datagen.git
(crontab -l 2>/dev/null; echo "* * * * * python /efs/datagen/dummy_data_gen.py -dt Sales -d '/efs/dummydata' -c 3000000 ") | crontab -

'''.format(id_Sales=id_Sales,efs=efs)
	with open(bash_file, 'w') as f:
		f.write(cmd) 

def run_remote_command(user, ip, pem_file, bash_file):
	cmd = 'ssh {user}@{ip} -i "{pem_file}" -oStrictHostKeyChecking=no  "bash -s" -- < {bash_file}'.format(user=user,ip=ip,pem_file=pem_file,bash_file=bash_file)
	print cmd
	os.system(cmd)
	print cmd

def return_public_ip(instance_filter = 'all'):
	out = []
	instances = get_instance_list('all')
	for instance in instances:
		if instance_filter == 'all' or instance_filter == instance.instance_id:
			out.append(instance.public_ip_address)
	return out
	
##############################################################################
#MAIN
##############################################################################

def main():
	bash_file = 'aws_instance_setup.sh'
	print 'Creating instance:--------'
	instance = ec2_create_instances('linux003', AllocateNewAddress = False, gb_list=None)
	instance_id = instance[0].instance_id
	#instance_id = 'i-0cf6f93df92564201'
	print 'instance_id: {instance_id}'.format(instance_id=instance_id)
	ip = return_public_ip(instance_id)[0]
	#ip='54.148.138.1'
	print 'IP: {ip}'.format(ip=ip)
	print 'Writing shell script to file: {bash_file}'.format(bash_file=bash_file)
	create_shell_script(bash_file)
	print 'Executing shell script-----'
	run_remote_command(user, ip, pem_file, bash_file)
	#ip = return_public_ip('i-00ab08520f677d7b2')
	#ec2_instances_print(detail=1,status_group='all',instance_filter='i-00ab08520f677d7b2')

if (__name__ == '__main__'):
    main()
