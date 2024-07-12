package utils

import (
	"github.com/aws/jsii-runtime-go"
)

func MountVolumeToPathAndPersist(volumeName string, path string) []*string {
	if volumeName == "" || path == "" {
		// we panic. cdk is build time only.
		panic("volumeName and path cannot be empty")
	}

	commands := jsii.Strings(
		"sudo mkfs -t xfs /dev/"+volumeName,
		"sudo mkdir -p "+path,
		"sudo mount /dev/"+volumeName+" "+path,
		"sudo chown ec2-user:ec2-user "+path,
		"echo '/dev/"+volumeName+" "+path+" xfs defaults 0 0' | sudo tee -a /etc/fstab",
	)

	return *commands
}

func MoveToPath(file string, path string) *string {
	if file == "" || path == "" {
		// we panic. cdk is build time only.
		panic("file and path cannot be empty")
	}

	command := jsii.String(fmt.Sprintf("sudo mv %s %s", file, path))
	return command
}
