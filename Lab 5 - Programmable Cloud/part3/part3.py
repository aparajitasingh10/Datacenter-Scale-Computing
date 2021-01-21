#!/usr/bin/env python3


#Name: Aparajita Singh
#Worked with: Amit Baran Roy

#References: 
#https://cloud.google.com/compute/docs/tutorials/python-guide
#https://github.com/GoogleCloudPlatform/python-docs-samples

import argparse
import os
import time
from pprint import pprint

from requests import get

from googleapiclient import discovery
import google.auth
from six.moves import input


credentials, project = google.auth.default()
service = discovery.build('compute', 'v1', credentials=credentials)

#
# Stub code - just lists all instances
#
def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None

#print("Your running instances are:")
#for instance in list_instances(service, project, 'us-west1-b'):
#    print(instance['name'])

#
# Stub code - creates an instances
#
#[START create_instance]
def create_instance(compute, project, zone, name, bucket):
    # Get the Ubuntu 1804 Lts image.
    image_response = compute.images().getFromFamily(
        project='ubuntu-os-cloud', family='ubuntu-1804-lts').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'startup-script.sh'), 'r').read()
    vm2ScriptAsString = open(
        os.path.join(
            os.path.dirname(__file__), 'startup-script-vm2.sh'), 'r').read()
    vm2Code = open(
        os.path.join(
            os.path.dirname(__file__), 'vm2code.py'), 'r').read()
    serviceCredentials = open(
        os.path.join(
            os.path.dirname(__file__), 'service-credentials.json'), 'r').read()
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }, {
                # Startup script for vm2
                # Downloaded to vm1 upon startup.
                'key': 'vm2-startup-script',
                'value': vm2ScriptAsString
            }, {
                # create instance python code for vm2
                # Downloaded to vm1 upon startup.
                'key': 'vm1-launch-vm2-code',
                'value': vm2Code
            }, {
                # service credentials for vm2
                # Downloaded to vm1 upon startup.
                'key': 'service-credentials',
                'value': serviceCredentials
            }, {
                'key': 'url',
                'value': image_url
            }, {
                'key': 'text',
                'value': image_caption
            }, {
                'key': 'bucket',
                'value': bucket
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()
# [END create_instance]

#
# Stub code - deletes an instances
#
# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
# [END delete_instance]

#
# Stub code - waiting for operation
#
# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)
# [END wait_for_operation]

#
# Stub code - main code that calls the creation/deletion of vm instances
#
# [START run]
def main(project, bucket, zone, instance_name, wait=True):
    #compute = googleapiclient.discovery.build('compute', 'v1')

    print('Creating instance.')

    operation = create_instance(service, project, zone, instance_name, bucket)
    wait_for_operation(service, project, zone, operation['name'])

    instances = list_instances(service, project, zone)

    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])

    print("""
Instance created.
It will take a minute or two for the instance to complete work.
Check this URL: http://storage.googleapis.com/{}/output.png
Once the image is uploaded press enter to delete the instance.
""".format(bucket))

    #Waiting for user input to delete the created instance    
    if wait:
        input()

    instances = list_instances(service, project, zone)

    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print('Deleting instance ' + instance['name'])
        operation = delete_instance(service, project, zone, instance['name'])
        wait_for_operation(service, project, zone, operation['name'])


#
# Stub code - main default function
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    #parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument(
        'bucket_name', help='Your Google Cloud Storage bucket name.')
    parser.add_argument(
        '--zone',
        default='us-central1-f',
        help='Compute Engine zone to deploy to.')
    parser.add_argument(
        '--name', default='demo-instance', help='New instance name.')

    args = parser.parse_args()

    main(project, args.bucket_name, args.zone, args.name)
# [END run]
