package operator

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	"os"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	configv1 "github.com/openshift/api/config/v1"
	configclient "github.com/openshift/client-go/config/clientset/versioned"
)

// EBSVolumeTagController is the custom controller
type EBSVolumeTagController struct {
	configClient configclient.Interface
	coreClient   corev1.CoreV1Interface
	queue        workqueue.RateLimitingInterface
	informer     cache.SharedIndexInformer
	awsEC2Client *ec2.EC2
}

// NewEBSVolumeTagController sets up the custom controller with informers and event handlers
func NewEBSVolumeTagController(configClient configclient.Interface, coreClient corev1.CoreV1Interface, awsEC2Client *ec2.EC2) (*EBSVolumeTagController, error) {
	// Create a workqueue for rate-limiting
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Create a custom listerWatcher for the Infrastructure resource
	listerWatcher := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return configClient.ConfigV1().Infrastructures().List(context.TODO(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.name", "cluster").String()
			return configClient.ConfigV1().Infrastructures().Watch(context.TODO(), options)
		},
	}

	// Create a shared informer for the Infrastructure resource
	informer := cache.NewSharedIndexInformer(
		listerWatcher,
		&configv1.Infrastructure{}, // The resource type
		time.Minute*10,             // Resync period
		cache.Indexers{},
	)

	// Set up the controller
	controller := &EBSVolumeTagController{
		configClient: configClient,
		coreClient:   coreClient,
		queue:        queue,
		informer:     informer,
		awsEC2Client: awsEC2Client,
	}

	// Add event handlers to the informer
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			controller.handleAdd(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			controller.handleUpdate(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			controller.handleDelete(obj)
		},
	})
	if err != nil {
		return nil, err
	}

	return controller, nil
}

// handleAdd is called when an Infrastructure resource is added
func (c *EBSVolumeTagController) handleAdd(obj interface{}) {
	infra := obj.(*configv1.Infrastructure)
	klog.Infof("Infrastructure resource added: %s", infra.Name)
	c.processInfrastructure(infra)
}

// handleUpdate is called when an Infrastructure resource is updated
func (c *EBSVolumeTagController) handleUpdate(oldObj, newObj interface{}) {
	oldInfra := oldObj.(*configv1.Infrastructure)
	newInfra := newObj.(*configv1.Infrastructure)

	klog.Infof("Infrastructure resource updated: %s", newInfra.Name)

	// Only restart if the Status.PlatformStatus.AWS.ResourceTags field has changed
	if !reflect.DeepEqual(oldInfra.Status.PlatformStatus.AWS.ResourceTags, newInfra.Status.PlatformStatus.AWS.ResourceTags) {
		klog.Infof("AWS ResourceTags changed: triggering processing and restart")
		c.processInfrastructure(newInfra)
		c.restartOperator() // Trigger the restart if ResourceTags have changed
	}
}

// handleDelete is called when an Infrastructure resource is deleted
func (c *EBSVolumeTagController) handleDelete(obj interface{}) {
	infra := obj.(*configv1.Infrastructure)
	klog.Infof("Infrastructure resource deleted: %s", infra.Name)
}

// processInfrastructure processes the Infrastructure resource, extracts the AWS information, and updates EBS tags
func (c *EBSVolumeTagController) processInfrastructure(infra *configv1.Infrastructure) {
	if infra.Status.PlatformStatus != nil && infra.Status.PlatformStatus.AWS != nil {
		awsInfra := infra.Status.PlatformStatus.AWS

		// Fetch all PVs and their associated volume IDs
		err := c.fetchPVsAndUpdateTags(awsInfra.ResourceTags)
		if err != nil {
			klog.Errorf("Error processing PVs for infrastructure update: %v", err)
		}
	}
}

// fetchPVsAndUpdateTags retrieves all PVs in the cluster, fetches the AWS EBS Volume IDs, and updates the tags
func (c *EBSVolumeTagController) fetchPVsAndUpdateTags(resourceTags []configv1.AWSResourceTag) error {
	// List all PVs in the cluster
	pvs, err := c.coreClient.PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("error fetching PVs: %v", err)
	}

	// Iterate over each PV, and get the AWS EBS volume ID
	for _, pv := range pvs.Items {
		// Check if the PV is an AWS EBS volume
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == "ebs.csi.aws.com" {
			volumeID := pv.Spec.CSI.VolumeHandle

			// Update the tags for this AWS EBS volume
			err = c.updateEBSTags(volumeID, resourceTags)
			if err != nil {
				klog.Errorf("Error updating tags for volume %s: %v", volumeID, err)
			} else {
				klog.Infof("Successfully updated tags for volume %s", volumeID)
			}
		}
	}

	return nil
}

// updateEBSTags merges new tags with existing ones for a given EBS volume using AWS SDK v1
func (c *EBSVolumeTagController) updateEBSTags(volumeID string, resourceTags []configv1.AWSResourceTag) error {
	// 1. Retrieve existing tags from the EBS volume
	existingTagsOutput, err := c.awsEC2Client.DescribeTags(&ec2.DescribeTagsInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("resource-id"),
				Values: []*string{aws.String(volumeID)},
			},
		},
	})
	if err != nil {
		return err
	}

	// Convert existing tags into a map
	existingTagMap := make(map[string]string)
	for _, tagDesc := range existingTagsOutput.Tags {
		existingTagMap[*tagDesc.Key] = *tagDesc.Value
	}

	// 2. Merge the new tags from infra.Status.PlatformStatus.AWS.ResourceTags
	for _, tag := range resourceTags {
		existingTagMap[tag.Key] = tag.Value
	}

	// Convert the tag map back to the AWS Tag format
	var mergedTags []*ec2.Tag
	for key, value := range existingTagMap {
		mergedTags = append(mergedTags, &ec2.Tag{
			Key:   aws.String(key),
			Value: aws.String(value),
		})
	}

	// 3. Apply the merged tags to the EBS volume
	_, err = c.awsEC2Client.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{aws.String(volumeID)},
		Tags:      mergedTags,
	})

	return err
}

// restartOperator triggers the restart of the operator by exiting the process
func (c *EBSVolumeTagController) restartOperator() {
	klog.Infof("Restarting the operator due to changes in ResourceTags...")
	os.Exit(1) // Exit the process to trigger a restart by Kubernetes
}

// Run starts the controller and processes events from the informer
func (c *EBSVolumeTagController) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	klog.Infof("Starting EBSVolumeTagController")
	go c.informer.Run(stopCh)

	// Wait for the informer caches to sync before processing
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		log.Fatal("Failed to sync caches")
		return
	}

	<-stopCh
	klog.Infof("Shutting down EBSVolumeTagController")
}
