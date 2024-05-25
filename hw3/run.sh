#!/bin/bash

aws emr create-cluster \
 --name "cindy cluster" \
 --log-uri "s3://aws-logs-549787090008-us-east-2/elasticmapreduce" \
 --release-label "emr-7.1.0" \
 --service-role "arn:aws:iam::549787090008:role/EMR_DefaultRole" \
 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-0ca32774ad00ed999","EmrManagedSlaveSecurityGroup":"sg-01b1848377d321647","AdditionalMasterSecurityGroups":[],"AdditionalSlaveSecurityGroups":[],"SubnetId":"subnet-013b0c5bdd6fd3339"}' \
 --applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=Livy Name=Spark \
 --instance-groups '[{"BidPrice":"0.100","InstanceCount":1,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"m4.large","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":1}]}},{"BidPrice":"0.100","InstanceCount":2,"InstanceGroupType":"TASK","Name":"Tasks","InstanceType":"m4.large","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":1}]}},{"BidPrice":"0.100","InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Primary","InstanceType":"m4.large","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":1}]}}]' \
 --bootstrap-actions '[{"Args":[],"Name":"pandas bootstrap","Path":"s3://de300spring2024/cindy_vong/bootstrap_pandas.sh"}]' \
 --steps '[{"Name":"hw3","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","--master","yarn","s3://de300spring2024/cindy_vong/hw3.py"],"Type":"CUSTOM_JAR"}]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --auto-termination-policy '{"IdleTimeout":18000}' \
 --region "us-east-2"
