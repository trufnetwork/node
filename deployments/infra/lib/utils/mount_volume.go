package utils

import "github.com/aws/jsii-runtime-go"

func MountVolumeToPathAndPersist(volumeName string, path string) []*string {
	return *jsii.Strings(
		"sudo mkfs -t xfs /dev/"+volumeName,
		"sudo mkdir "+path,
		"sudo mount /dev/"+volumeName+" "+path,
		"sudo chown ec2-user:ec2-user "+path,
		"echo '/dev/"+volumeName+" "+path+" xfs defaults 0 0' | sudo tee -a /etc/fstab",
	)
}

func MoveToPath(file string, path string) *string {
	return jsii.String(
		"sudo mv " + file + " " + path,
	)
}
