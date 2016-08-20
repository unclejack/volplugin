package ebs

import (
	"fmt"

	"github.com/contiv/errored"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func getInstanceID() (string, error) {
	c := ec2metadata.New(session.New())
	instanceID, err := c.GetMetadata("instance-id")
	if err != nil {
		return "", errored.Errorf("failed to retrieve the instance id: %v", err)
	}
	return instanceID, nil
}

func findFreeBlockDevice(mappings []*ec2.InstanceBlockDeviceMapping) (string, error) {
	var dev uint8
	for dev = 'a'; dev < 'z'; dev++ {
		blockDev := fmt.Sprintf("/dev/xvd%c", dev)
		usable := true
		for _, mapping := range mappings {
			if blockDev == *mapping.DeviceName {
				usable = false
				break
			}

		}
		if usable {
			return blockDev, nil
		}
	}

	return "", errored.Errorf("failed to find free block device")

}

func findBlockVolumeBlockDevice(mappings []*ec2.InstanceBlockDeviceMapping, volume string) (string, error) {
	var dev string
	for _, mapping := range mappings {
		if volume == *mapping.Ebs.VolumeId {
			dev = *mapping.DeviceName
			return dev, nil
		}
	}

	return "", errored.Errorf("failed to find volume in attached volumes")
}
