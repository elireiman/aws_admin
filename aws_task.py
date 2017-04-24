import boto3, uuid, os, argparse, sys, time, random, parameters
from types import FunctionType
from operator import itemgetter

"""
v20170419a
OVERVIEW:  This script performs actions on a filtered list of AWS instances
	Use -ft for filter_type and -ft for filter_value.  If not supplied - it will return an unfiltered list
	
EXAMPLE FOR parameters.py:
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
# CLI PARAMETERS
##############################################################################
parser = argparse.ArgumentParser()
parser.add_argument("-ft", "--filter_type", help="Filter Type, must be one of the following: 'availability-zone','image-id','instance-id','instance-state-name','instance-type','ip-address','instance.group-id','instance.group-name','placement-group-name','private-ip-address','tag-key','tag-value','vpc-id' ", default='all')
parser.add_argument("-fv", "--filter_value", help="Can be a single value, or a list / tuple", default = 'all')
parser.add_argument("-a", "--action", help="Action to take -- must be one of the following: 'start','stop','terminate','reboot','run_script'", default = None)
parser.add_argument("-i", "--display_info", help="Display info about instances?", action="store_true")
args = parser.parse_args()

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
#session = boto3.Session(profile_name='default')
ec2_client = boto3.client('ec2',region_name=region_name)
ec2 = boto3.resource('ec2',region_name=region_name)

##############################################################################
# FUNCTIONS:
##############################################################################

def make_list_if_not_already(value):
	if type(value) is list or type(value) is tuple:
		return value
	else:
		return [value]

def get_instances(filter_type='all',filter_value='all'):
	instances = ec2.instances.all()
	if filter_type in ['all',['all']]:
		return instances
	#if filter_type in ['tag-value']:
	if filter_type not in ('availability-zone','image-id','instance-id','instance-state-name'
							,'instance-type','ip-address','instance.group-id','instance.group-name'
							,'placement-group-name','private-ip-address','tag-key','tag-value','vpc-id','all'):
		print("ERROR:  filter_value must be in this list: 'all', 'availability-zone','image-id','instance-id','instance-state-name','instance-type','ip-address','instance.group-id','instance.group-name','placement-group-name','private-ip-address','tag-key','tag-value','vpc-id'")
		return None
	filter_value = make_list_if_not_already(filter_value)
	return ec2.instances.filter(Filters=[{'Name': filter_type, 'Values': filter_value}])

def get_instance_ids_from_instances(instances):
	ids = []
	for instance in instances:
		ids.append(instance.instance_id)
	return ids

def start_instances(instances):
	dat=time.strftime("%Y-%m-%d %H:%M:%S")
	ids = get_instance_ids_from_instances(instances)
	print('{} - NOTICE: STARTING INSTANCES: {}'.format(dat,ids))
	response = ec2_client.start_instances(InstanceIds=ids)
	return response

def stop_instances(instances):
	dat=time.strftime("%Y-%m-%d %H:%M:%S")
	ids = get_instance_ids_from_instances(instances)
	print('{} - NOTICE: STOPPING INSTANCES: {}'.format(dat,ids))
	response = ec2_client.stop_instances(InstanceIds=ids)
	return response

def build_volume_dict(instances):
	instance_volumes = {}
	for instance in instances:
		instance_volumes[instance.instance_id] = []
		volumes = instance.volumes.all()
		for volume in volumes:
			instance_volumes[instance.instance_id].append( {
				'volume_id': volume.id
				, 'size': volume.size
				, 'iops': volume.iops
				, 'state': volume.state
				, 'volume_type': volume.volume_type
				, 'device_mount': volume.attachments[0]['Device']
				})
	# if print_volumes:
	# 	print('# VOLUME LIST: ###########################')
	# 	for instance, volume_list in instance_volumes.iteritems():
	# 		#volume_list_sorted = volume_list.sort(key=operator.itemgetter('device_mount'))
	# 		volume_list_sorted = sorted(volume_list, key=itemgetter('device_mount')) 
	# 		for volume in volume_list_sorted:
	# 			print('instance: {instance}, device_mount: {device_mount}, size: {size}, iops: {iops}, volume_id: {volume_id}').format(
	# 			instance=instance, device_mount=volume['device_mount'], size=volume['size'], iops=volume['iops']
	# 			, volume_id=volume['volume_id']
	# 			)
	return instance_volumes
	
def print_info(instances, volume_dict = None):
	for instance in instances:
		security_groups_list = []
		for security_group_dict in instance.security_groups:
			security_groups_list.append(security_group_dict['GroupName'])
		tags_list = []
		for tags_dict in instance.tags:
			tags_list.append(tags_dict['Value'])

		# if volume_dict:
		volume_string_list = []
		volume_list = volume_dict[instance.instance_id]
		volume_list_sorted = sorted(volume_list, key=itemgetter('device_mount')) 
		for volume in volume_list_sorted:
			volume_string_list.append('device_mount: {device_mount}, size: {size}, iops: {iops}, volume_id: {volume_id}'.format(
				device_mount=volume['device_mount'], size=volume['size'], iops=volume['iops'], volume_id=volume['volume_id']
				))

		print """
instance_id: {instance_id} -------------------
placement_group: {placement_group} - placement_AZ: {placement_AZ} - key_name: {key_name}
instance_id: {instance_id} - state: {state} - instance_type: {instance_type} - image_id: {image_id}
launch_time: {launch_time}
security_groups: {security_groups}
tags: {tags}
public_IP: {public_ip_address} - private_IP: {private_ip_address} - subnet_id: {subnet}
root_device: {root_device_name} - type: {root_device_type}
{volumes}
""".format(
	placement_group=instance.placement_group.name ,placement_AZ=instance.placement['AvailabilityZone'],key_name=instance.key_name
	,instance_id=instance.instance_id,state=instance.state['Name'],instance_type=instance.instance_type,image_id=instance.image_id
	,launch_time=instance.launch_time
	,security_groups=','.join(security_groups_list)
	,tags=','.join(tags_list)
	,public_ip_address=instance.public_ip_address,private_ip_address=instance.private_ip_address, subnet=instance.subnet.id
	,root_device_name=instance.root_device_name,root_device_type=instance.root_device_type
	,volumes='\n'.join(volume_string_list)
	#, vpc=instance.vpc
	#,vpc_id=instance.vpc_id
	#,key_pair=instance.key_pair
	#,network_interfaces=instance.network_interfaces
	#,ami_launch_index=instance.ami_launch_index
	#,vpc_addresses=instance.vpc_addresses
	#,platform=instance.platform
	# placement_group['name']
	)

def main():
	instances = get_instances(filter_type=args.filter_type,filter_value=args.filter_value)
	volume_dict = build_volume_dict(instances)
	#print volume_dict
	if args.display_info:
		print_info(instances,volume_dict)


if (__name__ == '__main__'):
    main()



# Instance States: pending | running | shutting-down | terminated | stopping | stopped 


#instances = ec2.instances

#for instance in instance

#instances = get_instances()
# instances = get_instances('tag-value','Linux')
# instances = get_instances('tag-value','abcde')
# instances = get_instances('image-id','ami-5323b533')
# instances = get_instances('instance-type','t2.micro')
#response = start_instances('instance-type','t2.micro')



'''
	instance_list = []
	if filter_type not in [
			  'all', 'image_id', 'instance_id', 'instance_type'
			, 'status', 'tag' ,'private_ip_address', 'public_ip_address'
			, 'placement', 'key_name'
			]:
		print("ERROR: filter_type must be one of the following: ['all','image_id', 'instance_id', 'instance_type', 'status', 'tag' ,'private_ip_address', 'public_ip_address', 'placement', 'key_name'] ")
		return instance_list
	instances = ec2.instances.all()
	if filter_type=='all':
		return instances
	for instance in instances:
		#simple filter types -- no complex data types
		if filter_type in ['image_id', 'instance_id', 'instance_type', 'placement'
			, 'key_name','private_ip_address', 'public_ip_address']:
			if 
		#instance_list.append(instance)
		
		# state=instance.state['Name']

'''