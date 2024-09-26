/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	localv1 "github.com/openshift/local-storage-operator/pkg/common"
	ocsoperatorv1 "github.com/red-hat-storage/ocs-operator/api/v1"
	"github.com/red-hat-storage/ocs-operator/controllers/defaults"
	ocsutil "github.com/red-hat-storage/ocs-operator/controllers/util"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/ceph/cluster/osd"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubernetes/pkg/kubelet"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	podStateReasonCrashLoopBackOff = "CrashLoopBackOff"
)

// NodeRecovery represents the structure of the object that recovers an ODF cluster from a physical failure
type NodeRecovery struct {
	client.Client
	*rest.Config
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=pods/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Pod object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *NodeRecovery) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	var err error
	ready, err := r.isStorageClusterReady()
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		// cluster has recovered
		enabled, err := r.isCephToolsEnabled()
		if err != nil {
			return ctrl.Result{}, err
		}
		if enabled {
			err = r.disableCephTools()
		}
		return ctrl.Result{}, err
	}
	status, err := r.getCephHealthStatus()
	if err != nil {
		return ctrl.Result{}, err
	}
	if status == cephclient.CephHealthOK {

	}
	enabled, err := r.isCephToolsEnabled()
	if err != nil {
		return ctrl.Result{}, err
	}
	if !enabled {
		err = r.enableCephTools()
		return ctrl.Result{}, err
	}
	p, err := r.getCephToolsPodPhase()
	if err != nil {
		return ctrl.Result{}, err
	}

	if p != v1.PodRunning {
		log.Log.V(5).Info("Waiting for Ceph Tools pod to be in Running phase %s")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	pods, err := r.getPodsInPhase(v1.PodFailed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(pods.Items) > 0 {
		for _, p := range pods.Items {
			err := r.managePod(p)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	pods, err = r.getPodsInPhase(v1.PodPending)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(pods.Items) > 0 {
		for _, p := range pods.Items {
			err := r.managePod(p)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeRecovery) SetupWithManager(mgr ctrl.Manager) error {

	resourceCreateOrUpdate := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}

	return ctrl.NewControllerManagedBy(mgr).
		// WithEventFilter(resourceHasFilterLabel(nil, "a")).
		Named("odf-pod-recovery-controller").
		WatchesMetadata(&corev1.Pod{}, &handler.EnqueueRequestForObject{}, builder.WithPredicates(resourceCreateOrUpdate)).
		Watches(&ocsoperatorv1.StorageCluster{}, &handler.EnqueueRequestForObject{}, builder.WithPredicates(resourceCreateOrUpdate)).
		Complete(r)
}

// ResourceHasFilterLabel returns a predicate that returns true only if the provided resource contains
// a label with the WatchLabel key and the configured label value exactly.
// func resourceHasFilterLabel(logger logr.Logger, labelValue string) predicate.Funcs {
// 	return predicate.Funcs{
// 		UpdateFunc: func(e event.UpdateEvent) bool {
// 			return processIfLabelMatch(logger.WithValues("predicate", "ResourceHasFilterLabel", "eventType", "update"), e.ObjectNew, labelValue)
// 		},
// 		CreateFunc: func(e event.CreateEvent) bool {
// 			return processIfLabelMatch(logger.WithValues("predicate", "ResourceHasFilterLabel", "eventType", "create"), e.Object, labelValue)
// 		},
// 		DeleteFunc: func(e event.DeleteEvent) bool {
// 			return processIfLabelMatch(logger.WithValues("predicate", "ResourceHasFilterLabel", "eventType", "delete"), e.Object, labelValue)
// 		},
// 		GenericFunc: func(e event.GenericEvent) bool {
// 			return processIfLabelMatch(logger.WithValues("predicate", "ResourceHasFilterLabel", "eventType", "generic"), e.Object, labelValue)
// 		},
// 	}
// }

//
// WAIT FOR PODS TO STABILIZE
//

func (r *NodeRecovery) waitForPodsToStabilize() error {

	pods := &v1.PodList{}
	err := r.List(context.Background(), pods, &client.ListOptions{Namespace: ODF_NAMESPACE})
	if err != nil {
		return err
	}
	var errs error
	for _, pod := range pods.Items {
		for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
			if status.State.Waiting != nil &&
				((status.State.Waiting.Reason == kubelet.ContainerCreating) || (status.State.Waiting.Reason == kubelet.PodInitializing)) {
				errs = errors.Join(errs, fmt.Errorf("Pod %s: container %s waiting in %d: %s", pod.Name, status.Name, status.State.Waiting.Reason, status.State.Waiting.Message))
			}
		}
	}
	return errs
}

func (r *NodeRecovery) isStorageClusterReady() (bool, error) {

	status, err := r.getStorageClusterStatus()
	if err != nil {
		return false, err
	}
	return status.Phase == ocsutil.PhaseReady, nil
}

func (r *NodeRecovery) getStorageClusterStatus() (*ocsoperatorv1.StorageClusterStatus, error) {
	ctx := context.Background()
	l := &ocsoperatorv1.StorageClusterList{}

	err := r.List(ctx, l, &client.ListOptions{Namespace: ODF_NAMESPACE})
	if err != nil {
		return nil, err
	}
	if len(l.Items) == 0 {
		return nil, fmt.Errorf("No storagecluster instance found in %s", ODF_NAMESPACE)
	}
	return &l.Items[0].Status, nil
}

func (r *NodeRecovery) getRunningCephToolsPod() (*v1.Pod, error) {
	ctx := context.Background()
	l := &v1.PodList{}
	selectorMap := map[string]string{"app": "rook-ceph-tools"}
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: selectorMap})
	if err != nil {
		return nil, err
	}
	err = r.List(ctx, l, &client.ListOptions{Namespace: ODF_NAMESPACE, LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	if len(l.Items) == 0 {
		return nil, fmt.Errorf("No pods found with label %v", selectorMap)
	}
	return &l.Items[0], nil
}

func (r *NodeRecovery) archiveCephDaemonCrashMessages() error {
	cmd := []string{"ceph", "crash", "archive-all"}
	pod, err := r.getRunningCephToolsPod()
	if err != nil {
		return err
	}
	_, _, err = r.executeRemoteCommand(&types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, cmd)
	return err
}

// executeRemoteCommand executes a remote shell command on the given namespaced pod
// returns the output from stdout and stderr
func (r *NodeRecovery) executeRemoteCommand(nsName *types.NamespacedName, cmd []string) (string, string, error) {
	c := kubernetes.NewForConfigOrDie(r.Config)

	buf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	request := c.RESTClient().
		Post().
		Namespace(nsName.Namespace).
		Resource("pods").
		Name(nsName.Name).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Command: cmd,
			Stdin:   false,
			Stdout:  true,
			Stderr:  true,
			TTY:     true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(r.Config, "POST", request.URL())
	ctx := context.Background()
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: buf,
		Stderr: errBuf,
	})
	if err != nil {
		return "", "", fmt.Errorf("Failed executing command %s on %v/%v", cmd, nsName.Namespace, nsName.Name)
	}

	return buf.String(), errBuf.String(), nil
}

func (r *NodeRecovery) isCephToolsEnabled() (bool, error) {
	o, err := r.getOSCInitialization()
	if err != nil {
		return false, err
	}
	return o.Spec.EnableCephTools == true, nil
}

// setEnableCephToolsValue configures the ocsinitialization instance in the cluster to enable or disable the deployment of the ceph tools pod
func (r *NodeRecovery) setEnableCephToolsValue(value bool) error {
	o, err := r.getOSCInitialization()
	if err != nil {
		return err
	}
	o.Spec.EnableCephTools = value
	return r.Update(context.Background(), o, &client.UpdateOptions{})
}

func (r NodeRecovery) getOSCInitialization() (*ocsoperatorv1.OCSInitialization, error) {
	ctx := context.Background()
	o := &ocsoperatorv1.OCSInitialization{}
	err := r.Get(ctx, types.NamespacedName{Namespace: ODF_NAMESPACE, Name: "ocsinit"}, o, &client.GetOptions{})
	return o, err
}

func (r *NodeRecovery) enableCephTools() error {
	return r.setEnableCephToolsValue(true)
}

func (r *NodeRecovery) disableCephTools() error {
	return r.setEnableCephToolsValue(false)
}

func (r *NodeRecovery) getCephToolsPodPhase() (v1.PodPhase, error) {
	pod, err := r.getRunningCephToolsPod()
	if err != nil {
		return "", err
	}
	return pod.Status.Phase, nil
}

func (r *NodeRecovery) getPodsInPhase(phase v1.PodPhase) (*v1.PodList, error) {
	l := &v1.PodList{}
	err := r.List(context.Background(), l, &client.ListOptions{FieldSelector: fields.ParseSelectorOrDie(`status.phase=="` + string(phase) + `"`)})
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (r *NodeRecovery) getStorageClusterPhase() (string, error) {
	status, err := r.getStorageClusterStatus()
	if err != nil {
		return "", err
	}
	return status.Phase, nil
}

// getCephHealthStatus retrieves the health status of Ceph by querying the ceph client in the ceph tools pods
func (r *NodeRecovery) getCephHealthStatus() (string, error) {
	p, err := r.getRunningCephToolsPod()
	if err != nil {
		return "", err
	}
	stdout, stderr, err := r.executeRemoteCommand(&types.NamespacedName{Namespace: ODF_NAMESPACE, Name: p.Name}, []string{"ceph", "status", "-f", "json", "|", "jq", "-r", "'.health.status'"})
	if err != nil {
		return "", err
	}
	if len(stderr) > 0 {
		return "", fmt.Errorf("Error while executing remote shell to retrieve the ceph health status: %s", stderr)
	}
	return stdout, nil
}

// getPodsInFailedPhaseWithReasonNodeAffinity retrieves a slice of pods that are in failed phase and the reason is due to NodeAffinity
func (r *NodeRecovery) getPodsInFailedPhaseWithReasonNodeAffinity() ([]v1.Pod, error) {
	l := &v1.PodList{}
	failedPods := []v1.Pod{}
	err := r.List(context.Background(), l, &client.ListOptions{Namespace: ODF_NAMESPACE, FieldSelector: fields.ParseSelectorOrDie(`status.phase=="Failed"`)})
	if err != nil {
		return nil, err
	}
	for _, pod := range l.Items {
		if pod.Status.Reason == names.NodeAffinity {
			failedPods = append(failedPods, pod)
		}
	}
	return failedPods, nil
}

// deleteFailedPodsWithReasonNodeAffinity deletes pods that are in failed phase and the reason is due to NodeAffinity
func (r *NodeRecovery) deleteFailedPodsWithReasonNodeAffinity() error {
	failedPods, err := r.getPodsInFailedPhaseWithReasonNodeAffinity()
	if err != nil {
		return err
	}
	var errs error
	for _, pod := range failedPods {
		err = r.Delete(context.Background(), &pod, &client.DeleteOptions{})
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// PENDING STATE PODS
//
// labelNodesWithPodsInPendingState labels the nodes where pods are in pending state
func (r *NodeRecovery) labelNodesWithPodsInPendingState() error {
	pods, err := r.getPodsInPhase(v1.PodPending)
	if err != nil {
		return err
	}
	nodes := map[string]struct{}{}
	for _, p := range pods.Items {
		nodes[p.Spec.NodeSelector[v1.LabelHostname]] = struct{}{}
	}
	var errs error
	for nodeName, _ := range nodes {
		n := &v1.Node{}
		err = r.Get(context.Background(), types.NamespacedName{Name: nodeName}, n, &client.GetOptions{})
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		n.Labels[defaults.NodeAffinityKey] = ""
		err = r.Update(context.Background(), n, &client.UpdateOptions{})
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// INITCRASHLOOKBACKOFF STATE PODS
//   - name: Get pods stuck in CrashLoopBackOff or Error status
//     shell: "oc get -n openshift-storage pods -l app=rook-ceph-osd --no-headers | grep 'CrashLoopBackOff\\|Error' | awk '{print $1}'"
//     register: crash_pods
//     until: crash_pods.rc == 0
//     retries: 30
//     delay: 10
func (r *NodeRecovery) getPodsInCrashLoopBackOff() ([]v1.Pod, error) {
	l := &v1.PodList{}
	pods := []v1.Pod{}
	err := r.List(context.Background(), l, &client.ListOptions{Namespace: ODF_NAMESPACE})
	if err != nil {
		return nil, err
	}
	for _, pod := range l.Items {
		for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
			if status.State.Waiting != nil && (status.State.Waiting.Reason == podStateReasonCrashLoopBackOff) {
				pods = append(pods, pod)
			}
		}
	}
	return pods, nil
}

type nodeDevice struct {
	nodeName   string
	deviceName string
}

// getNodeDeviceNameFromPV returns a struct that contain the node and device name associated to the PV used by the pod
// - name: Get pvc's
//   shell: "oc get -n openshift-storage pod/{{ item }} -o jsonpath='{.metadata.labels.ceph\\.rook\\.io\/pvc}'"
//   register: crash_pods_pvcs
//   until: crash_pods.rc == 0
//   with_items: "{{ crash_pods.stdout_lines }}"
//   retries: 30
//   delay: 10

// - name: Get pv's
//   shell: "oc get -n openshift-storage pvc/{{ item.stdout }} -o jsonpath='{.spec.volumeName}'"
//   register: crash_pods_pvs
//   until: crash_pods_pvs.rc == 0
//   with_items: "{{ crash_pods_pvcs.results }}"
//   retries: 30
//   delay: 10

//   - name: Get devices
//     shell: "oc get pv/{{ item.stdout }} -o jsonpath='{.metadata.labels.kubernetes\\.io/hostname}{\",\"}{.metadata.annotations.storage\\.openshift\\.com\/device-name}'"
//     register: osd_pods_devices
//     until: osd_pods_devices.rc == 0
//     with_items: "{{ crash_pods_pvs.results }}"
//     retries: 30
//     delay: 10

func (r *NodeRecovery) getNodeDeviceNameFromPV(pod *v1.Pod) (*nodeDevice, error) {
	if pvcName, ok := pod.Labels[osd.OSDOverPVCLabelKey]; ok {
		pvc := &v1.PersistentVolumeClaim{}
		err := r.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pvcName}, pvc, &client.GetOptions{})
		if err != nil {
			return nil, err
		}
		pv := &v1.PersistentVolume{}
		err = r.Get(context.Background(), types.NamespacedName{Name: pvc.Spec.VolumeName}, pv, &client.GetOptions{})
		if err != nil {
			return nil, err
		}
		if dn, ok := pv.Annotations[localv1.PVDeviceNameLabel]; ok {
			return &nodeDevice{nodeName: pvc.Labels[v1.LabelHostname], deviceName: dn}, nil
		}
		return nil, fmt.Errorf("Annotation %s not found in PV %s", localv1.PVDeviceNameLabel, pv.Name)
	}
	return nil, nil
}

// scaleRookCephOSDDeploymentsToZero scales all deployment objects that match the label `app=rook-ceph-osd` to zero
//   - name: Scale to 0
//     shell: "oc scale -n openshift-storage deployment rook-ceph-osd-{{ item | regex_search('rook-ceph-osd-([0-9]+)', '\\1') | first }} --replicas=0"
//     with_items: "{{ crash_pods.stdout_lines }}"
//     when: 'crash_pods.stdout_lines | length > 0'
//     retries: 30
//     delay: 10
func (r *NodeRecovery) scaleRookCephOSDDeploymentsToZero() error {
	l := &appsv1.DeploymentList{}
	selectorMap := map[string]string{"app": "rook-ceph-osd"}
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: selectorMap})
	if err != nil {
		return err
	}
	err = r.List(context.Background(), l, &client.ListOptions{Namespace: ODF_NAMESPACE, LabelSelector: selector})
	if err != nil {
		return err
	}
	for _, d := range l.Items {
		d.Spec.Replicas = ptr.To(int32(0))
		err = r.Update(context.Background(), &d, &client.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

// forceDeleteRookCephOSDPods forces deleting the pods belonging to the Rook Ceph OSD deployments that are stuck and unable to delete after scaling their deployments to zero
// - name: Check if osd pod is still running
//   shell: "oc get -n openshift-storage pods -l ceph-osd-id={{ item | regex_search('rook-ceph-osd-([0-9]+)', '\\1') | first }} -o custom-columns=name:metadata.name --no-headers"
//   register: result
//   with_items: "{{ crash_pods.stdout_lines }}"
//   when: 'crash_pods.stdout_lines | length > 0'
//   retries: 30
//   delay: 10

//   - name: Force delete if still running
//     shell: "oc delete -n openshift-storage pod {{ item.stdout }} --grace-period=0 --force"
//     with_items: "{{ result.results }}"
//     when: 'crash_pods.stdout_lines | length > 0'
//     ignore_errors: true
//     retries: 30
//     delay: 10
func (r *NodeRecovery) forceDeleteRookCephOSDPods() error {
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "ceph-osd-id", Operator: metav1.LabelSelectorOpExists},
		}},
	)
	if err != nil {
		return err
	}
	return r.DeleteAllOf(context.Background(), &v1.Pod{},
		&client.DeleteAllOfOptions{
			ListOptions:   client.ListOptions{Namespace: ODF_NAMESPACE, LabelSelector: selector},
			DeleteOptions: client.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))},
		})
}

// eraseDevices wipes out the devices associated to the PVs used by the crashing pods
//   - name: Erase devices before adding to OCS cluster
//     shell: "oc debug node/{{ item.name }} --image=registry.redhat.io/rhel8/support-tools -- chroot /host sgdisk --zap-all /dev/{{ item.device }}"
//     register: osd_pods_devices_sgdisk
//     until: osd_pods_devices_sgdisk.rc == 0
//     with_items: "{{ sgdisk }}"
//     retries: 30
//     delay: 10
func (r *NodeRecovery) eraseDevice(nd *nodeDevice) error {

	return nil
}

func (r *NodeRecovery) restartStorageOperator() error {
	// sleep for 15 seconds
	time.Sleep(15 * time.Second)

	return nil
}
