package ebs

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/contiv/errored"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
)

type volumeConfig struct {
	availabilityZone string
	size             int64
	iops             int64
	volumeType       string
	snapshot         string
	device           string
}

var (
	errNilService = errors.New("service can't be nil")
	errNilConfig  = errors.New("config can't be nil")
	errTimeout    = errors.New("operation timed out")
	errNotExists  = errors.New("volume doesn't exist")
)

const contivVolumeKey = "contiv.io.volplugin.volume.name"

func setVolumeNameTag(volumeID, name string, svc *ec2.EC2) error {
	tags := []*ec2.Tag{
		{
			Key:   aws.String(contivVolumeKey),
			Value: aws.String(name),
		},
	}
	return tagVolume(volumeID, tags, svc)
}

func deleteVolumeNameTag(volumeID, name string, svc *ec2.EC2) error {
	tags := []*ec2.Tag{
		{
			Key:   aws.String(contivVolumeKey),
			Value: aws.String(name),
		},
	}
	return untagVolume(volumeID, tags, svc)
}

func tagVolume(volumeID string, tags []*ec2.Tag, svc *ec2.EC2) error {
	params := &ec2.CreateTagsInput{
		Resources: []*string{
			aws.String(volumeID),
		},
		Tags:   tags,
		DryRun: aws.Bool(false),
	}
	_, err := svc.CreateTags(params)

	if err != nil {
		return err
	}

	return nil
}

func untagVolume(volumeID string, tags []*ec2.Tag, svc *ec2.EC2) error {
	params := &ec2.DeleteTagsInput{
		Resources: []*string{
			aws.String(volumeID),
		},
		Tags:   tags,
		DryRun: aws.Bool(false),
	}
	_, err := svc.DeleteTags(params)

	if err != nil {
		return err
	}

	return nil
}

func getVolumeWithName(name string, svc *ec2.EC2) (string, error) {
	tagDesc, err := getVolumeWithTag(contivVolumeKey, name, svc)
	if err != nil {
		return "", err
	}

	return *tagDesc.ResourceId, nil
}

func getVolumesWithFilters(filters []*ec2.Filter, svc *ec2.EC2) ([]*ec2.TagDescription, error) {
	params := &ec2.DescribeTagsInput{
		DryRun:     aws.Bool(false),
		Filters:    filters,
		MaxResults: aws.Int64(6),
	}
	resp, err := svc.DescribeTags(params)
	return resp.Tags, err
}

func getVolumeWithTag(key, value string, svc *ec2.EC2) (*ec2.TagDescription, error) {
	filters := []*ec2.Filter{
		{
			Name: aws.String("resource-type"),
			Values: []*string{
				aws.String("volume"),
			},
		},
		{
			Name: aws.String("key"),
			Values: []*string{
				aws.String(key),
			},
		},
		{
			Name: aws.String("value"),
			Values: []*string{
				aws.String(value),
			},
		},
	}

	volumes, err := getVolumesWithFilters(filters, svc)
	if err != nil {
		return nil, err
	}

	if len(volumes) != 1 {
		return nil, errored.Errorf("expected one response, got %v", len(volumes))
	}
	return volumes[0], nil
}

func listVolumes(svc *ec2.EC2) ([]*ec2.TagDescription, error) {
	return getVolumesWithKey(contivVolumeKey, svc)
}

func getVolumesWithKey(key string, svc *ec2.EC2) ([]*ec2.TagDescription, error) {
	filters := []*ec2.Filter{
		{
			Name: aws.String("resource-type"),
			Values: []*string{
				aws.String("volume"),
			},
		},
		{
			Name: aws.String("key"),
			Values: []*string{
				aws.String(key),
			},
		},
	}

	volumes, err := getVolumesWithFilters(filters, svc)
	if err != nil {
		return nil, err
	}

	return volumes, nil
}

func createVolume(config *volumeConfig, svc *ec2.EC2) (*ec2.Volume, error) {
	if svc == nil {
		return nil, errNilService
	}

	if config == nil {
		return nil, errNilConfig
	}

	input := &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String(config.availabilityZone),
		Size:             aws.Int64(config.size),
		VolumeType:       aws.String(config.volumeType),
	}

	if config.iops != 0 {
		input.Iops = aws.Int64(config.iops)
	}

	resp, err := svc.CreateVolume(input)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func attachVolume(volume, instance, device string, svc *ec2.EC2) (*ec2.VolumeAttachment, error) {
	if svc == nil {
		return nil, errNilService
	}

	input := &ec2.AttachVolumeInput{
		Device:     aws.String(device),
		InstanceId: aws.String(instance),
		VolumeId:   aws.String(volume),
	}

	resp, err := svc.AttachVolume(input)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func detachVolume(volume, instance, device string, force bool, svc *ec2.EC2) (*ec2.VolumeAttachment, error) {
	if svc == nil {
		return nil, errNilService
	}

	input := &ec2.DetachVolumeInput{
		VolumeId:   aws.String(volume),
		Device:     aws.String(device),
		Force:      aws.Bool(force),
		InstanceId: aws.String(instance),
	}

	resp, err := svc.DetachVolume(input)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func createAndAttach(config *volumeConfig, instance, device string, svc *ec2.EC2) (*ec2.VolumeAttachment, error) {
	vol, err := createVolume(config, svc)
	if err != nil {
		return nil, err
	}

	respn, err := svc.DescribeVolumes(nil)
	if err != nil {
		return nil, err
	}

	fmt.Println(respn)

	volumeID := *vol.VolumeId
	attachment, err := attachVolume(volumeID, instance, device, svc)
	if err != nil {
		return nil, err
	}

	return attachment, nil
}

func deleteVolume(volume string, svc *ec2.EC2) error {
	if svc == nil {
		return errNilService
	}

	input := &ec2.DeleteVolumeInput{
		VolumeId: aws.String(volume),
	}

	// DeleteVolumeOutput has no exported fields and we ignore it
	_, err := svc.DeleteVolume(input)
	return err
}

func getInstanceInfo(instance string, svc *ec2.EC2) (*ec2.Instance, error) {
	if svc == nil {
		return nil, errNilService
	}

	input := &ec2.DescribeInstancesInput{
		InstanceIds: []*string{
			aws.String(instance),
		},
	}

	resp, err := svc.DescribeInstances(input)
	if err != nil {
		return nil, err
	}

	if len(resp.Reservations) != 1 {
		return nil, fmt.Errorf("expected exactly one reservation")
	}

	if len(resp.Reservations[0].Instances) != 1 {
		return nil, fmt.Errorf("expected exactly one instance")
	}

	return resp.Reservations[0].Instances[0], nil
}

func getVolumeInfo(volume string, svc *ec2.EC2) (*ec2.Volume, error) {
	if svc == nil {
		return nil, errNilService
	}

	input := &ec2.DescribeVolumesInput{
		VolumeIds: []*string{
			aws.String(volume),
		},
	}

	resp, err := svc.DescribeVolumes(input)
	if err != nil {
		return nil, err
	}

	if len(resp.Volumes) != 1 {
		return nil, fmt.Errorf("expected exactly one volume")
	}

	return resp.Volumes[0], nil
}

func createVolumeSynchronously(config *volumeConfig, svc *ec2.EC2, timeout time.Duration) (*ec2.Volume, error) {
	resp, err := createVolume(config, svc)
	if err != nil {
		return nil, err
	}

	if *resp.State == ec2.VolumeStateAvailable {
		return resp, nil
	}

	c := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		c <- true
	}()

	exponent := 1
	for {
		select {
		case <-c:
			return resp, errTimeout
		case <-time.After(time.Millisecond * 500 * time.Duration(exponent)):
			vol, err := getVolumeInfo(*resp.VolumeId, svc)
			if err == nil && *vol.State == ec2.VolumeStateAvailable {
				return vol, nil
			}
			exponent += 1
		}
	}
}

func attachVolumeSynchronously(volume, instance, device string, svc *ec2.EC2, timeout time.Duration) (*ec2.VolumeAttachment, error) {
	var attachmentState string
	resp, err := attachVolume(volume, instance, device, svc)
	if err != nil {
		return nil, err
	}

	if *resp.State == ec2.VolumeAttachmentStateAttached {
		return resp, nil
	}

	c := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		c <- true
	}()

	for {
		select {
		case <-c:
			return nil, errTimeout
		case <-time.After(time.Millisecond * 100):
			_, err := os.Stat(device)
			if err != nil && os.IsNotExist(err) {
				continue
			}
			vol, err := getVolumeInfo(*resp.VolumeId, svc)
			if len(vol.Attachments) == 1 {
				attachmentState = *vol.Attachments[0].State
			}
			if err == nil && attachmentState == ec2.VolumeAttachmentStateAttached {
				return vol.Attachments[0], nil
			}
		}
	}
}

func detachVolumeSynchronously(volume, instance, device string, force bool, svc *ec2.EC2, timeout time.Duration) (*ec2.Volume, error) {
	resp, err := detachVolume(volume, instance, device, force, svc)
	if err != nil {
		return nil, err
	}

	if *resp.State == ec2.VolumeAttachmentStateDetached {
		vol, err := getVolumeInfo(*resp.VolumeId, svc)
		if err != nil {
			return nil, err
		}
		return vol, nil
	}

	c := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		c <- true
	}()

	for {
		select {
		case <-c:
			return nil, errTimeout
		case <-time.After(time.Millisecond * 100):
			_, err := os.Stat(device)
			if err == nil {
				continue
			}
			vol, err := getVolumeInfo(*resp.VolumeId, svc)
			if err == nil && *vol.State == ec2.VolumeStateAvailable {
				return vol, nil
			}
		}
	}
}

func deleteVolumeSynchronously(volume string, svc *ec2.EC2, timeout time.Duration) error {
	err := deleteVolume(volume, svc)
	if err != nil {
		return err
	}

	vol, err := getVolumeInfo(volume, svc)
	if err != nil {
		return err
	}

	if *vol.State == ec2.VolumeStateDeleted {
		return nil
	}

	c := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		c <- true
	}()

	for {
		select {
		case <-c:
			return errTimeout
		case <-time.After(time.Millisecond * 500):
			vol, err = getVolumeInfo(volume, svc)
			if err == nil {
				continue
			}
			awsErr, ok := err.(awserr.Error)

			if ok && awsErr.Code() == "InvalidVolume.NotFound" {
				return nil
			}
		}
	}
}
