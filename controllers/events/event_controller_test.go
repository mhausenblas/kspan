package events

import (
	"testing"
	"time"

	o "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDeploymentRolloutWithManagedFields(t *testing.T) {
	g := o.NewWithT(t)

	var (
		deploy1    unstructured.Unstructured
		rs1, rs2   unstructured.Unstructured
		pod0, pod1 unstructured.Unstructured
	)
	mustParse(t, deploy1str, &deploy1)
	mustParse(t, replicaSet1str, &rs1)
	mustParse(t, replicaSet2str, &rs2)
	mustParse(t, pod0str, &pod0)
	mustParse(t, pod1str, &pod1)

	// There is no 'top-level' event here; the controller must synthesise one from the managed fields of the Deployment.

	tests := []struct {
		name       string
		perm       []int
		wantTraces []string
	}{
		{
			name: "scaledown-later",
			perm: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			wantTraces: []string{
				"0: kubectl Deployment.Update ",
				"1: deployment-controller Deployment.ScalingReplicaSet (0) Scaled up replica set hello-world-6b9d85fbd6 to 1",
				"2: replicaset-controller ReplicaSet.SuccessfulCreate (1) Created pod: hello-world-6b9d85fbd6-klpv2",
				"3: default-scheduler Pod.Scheduled (2) Successfully assigned default/hello-world-6b9d85fbd6-klpv2 to kind-control-plane",
				"4: kubelet Pod.Pulled (2) Container image \"nginx:1.19.2-alpine\" already present on machine",
				"5: kubelet Pod.Created (2) Created container hello-world",
				"6: kubelet Pod.Started (2) Started container hello-world",
				"7: deployment-controller Deployment.ScalingReplicaSet (0) Scaled down replica set hello-world-7ff854f459 to 0",
				"8: kubelet Pod.Killing (7) Stopping container hello-world",
				"9: replicaset-controller ReplicaSet.SuccessfulDelete (7) Deleted pod: hello-world-7ff854f459-kl4hq",
			},
		},
		{
			name: "scaledown-earlier",
			perm: []int{0, 6, 1, 2, 3, 4, 5, 7, 8},
			wantTraces: []string{
				"0: kubectl Deployment.Update ",
				"1: deployment-controller Deployment.ScalingReplicaSet (0) Scaled up replica set hello-world-6b9d85fbd6 to 1",
				"2: replicaset-controller ReplicaSet.SuccessfulCreate (1) Created pod: hello-world-6b9d85fbd6-klpv2",
				"3: default-scheduler Pod.Scheduled (2) Successfully assigned default/hello-world-6b9d85fbd6-klpv2 to kind-control-plane",
				"4: kubelet Pod.Pulled (2) Container image \"nginx:1.19.2-alpine\" already present on machine",
				"5: kubelet Pod.Created (2) Created container hello-world",
				"6: kubelet Pod.Started (2) Started container hello-world",
				"7: deployment-controller Deployment.ScalingReplicaSet (0) Scaled down replica set hello-world-7ff854f459 to 0",
				"8: kubelet Pod.Killing (7) Stopping container hello-world",
				"9: replicaset-controller ReplicaSet.SuccessfulDelete (7) Deleted pod: hello-world-7ff854f459-kl4hq",
			},
		},
	}

	threshold, err := time.Parse(time.RFC3339, deploymentUpdateEventsThresholdStr)
	g.Expect(err).NotTo(o.HaveOccurred())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, r, exporter, _ := newTestEventWatcher(&deploy1, &rs1, &rs2, &pod0, &pod1)
			defer r.stop()
			for _, index := range tt.perm {
				var event corev1.Event
				mustParse(t, deploymentUpdateEvents[index], &event)
				g.Expect(r.handleEvent(ctx, &event)).To(o.Succeed())
			}
			g.Expect(r.checkOlderPending(ctx, threshold)).To(o.Succeed())
			r.flushOutgoing(ctx, threshold)
			g.Expect(exporter.dump()).To(o.Equal(tt.wantTraces))
		})
	}
}

func Test2PodDeploymentRollout(t *testing.T) {
	g := o.NewWithT(t)

	// Note: we can't inject two different versions of the Deployment
	// (before and after) into FakeClient, so we only do 'after'.
	var (
		deploy2                unstructured.Unstructured
		rs1, rs2               unstructured.Unstructured
		pod1, pod2, pod3, pod4 unstructured.Unstructured
	)
	mustParse(t, p2deployment2, &deploy2)
	mustParse(t, p2replicaSet1str, &rs1)
	mustParse(t, p2replicaSet2str, &rs2)
	mustParse(t, p2pod1str, &pod1)
	mustParse(t, p2pod2str, &pod2)
	mustParse(t, p2pod3str, &pod3)
	mustParse(t, p2pod4str, &pod4)

	tests := []struct {
		name       string
		perm       []int
		wantTraces []string
	}{
		{
			name: "straight",
			perm: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			wantTraces: []string{
				"0: kubectl-client-side-apply Deployment.Update ",
				"1: replicaset-controller ReplicaSet.SuccessfulDelete (0) Deleted pod: bryan-podinfo-787c9986b5-tkd9p",
				"2: kubelet Pod.Killing (1) Stopping container podinfod",
				"3: deployment-controller Deployment.ScalingReplicaSet (0) Scaled up replica set bryan-podinfo-5c5df9754b to 1",
				"4: replicaset-controller ReplicaSet.SuccessfulCreate (3) Created pod: bryan-podinfo-5c5df9754b-4w2hj",
				"5: default-scheduler Pod.Scheduled (4) Successfully assigned default/bryan-podinfo-5c5df9754b-4w2hj to kind-control-plane",
				"6: kubelet Pod.Pulling (4) Pulling image \"ghcr.io/stefanprodan/podinfo:5.0.3\"",
				"7: kubelet Pod.Pulled (4) Successfully pulled image \"ghcr.io/stefanprodan/podinfo:5.0.3\" in 7.556422631s",
				"8: kubelet Pod.Created (4) Created container podinfod",
				"9: kubelet Pod.Started (4) Started container podinfod",
				"10: deployment-controller Deployment.ScalingReplicaSet (0) Scaled down replica set bryan-podinfo-787c9986b5 to 1",
				"11: deployment-controller Deployment.ScalingReplicaSet (0) Scaled up replica set bryan-podinfo-5c5df9754b to 2",
				"12: replicaset-controller ReplicaSet.SuccessfulCreate (11) Created pod: bryan-podinfo-5c5df9754b-bhj4w",
				"13: default-scheduler Pod.Scheduled (12) Successfully assigned default/bryan-podinfo-5c5df9754b-bhj4w to kind-control-plane",
				"14: kubelet Pod.Pulling (12) Pulling image \"ghcr.io/stefanprodan/podinfo:5.0.3\"",
				"15: kubelet Pod.Pulled (12) Successfully pulled image \"ghcr.io/stefanprodan/podinfo:5.0.3\" in 8.129591184s",
				"16: kubelet Pod.Created (12) Created container podinfod",
				"17: kubelet Pod.Started (12) Started container podinfod",
				"18: deployment-controller Deployment.ScalingReplicaSet (0) Scaled down replica set bryan-podinfo-787c9986b5 to 0",
				"19: kubelet Pod.Killing (18) Stopping container podinfod",
				"20: replicaset-controller ReplicaSet.SuccessfulDelete (18) Deleted pod: bryan-podinfo-787c9986b5-fws9t",
			},
		},
	}

	threshold, err := time.Parse(time.RFC3339, p2deploymentUpdateEventsThresholdStr)
	g.Expect(err).NotTo(o.HaveOccurred())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, r, exporter, _ := newTestEventWatcher(&deploy2, &rs1, &rs2, &pod1, &pod2, &pod3, &pod4)
			defer r.stop()
			for _, index := range tt.perm {
				var event corev1.Event
				mustParse(t, p2deploymentUpdateEvents[index], &event)
				g.Expect(r.handleEvent(ctx, &event)).To(o.Succeed())
			}
			g.Expect(r.checkOlderPending(ctx, threshold)).To(o.Succeed())
			r.flushOutgoing(ctx, threshold)
			g.Expect(exporter.dump()).To(o.Equal(tt.wantTraces))
		})
	}
}

func TestDeploymentRolloutFromFlux(t *testing.T) {
	g := o.NewWithT(t)

	var (
		deploy1  unstructured.Unstructured
		rs1, rs2 unstructured.Unstructured
		pod1     unstructured.Unstructured
	)
	mustParse(t, fluxDeploymentStr, &deploy1)
	mustParse(t, fluxReplicaSet1astr, &rs1)
	mustParse(t, fluxReplicaSet1bstr, &rs2)
	mustParse(t, fluxPod1astr, &pod1)

	tests := []struct {
		name       string
		perm       []int
		wantTraces []string
	}{
		{
			name: "flux-event-later",
			perm: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			wantTraces: []string{
				"0: flux Deployment.Sync Commit e332e7bac962: Update nginx",
				"1: deployment-controller Deployment.ScalingReplicaSet (0) Scaled up replica set hello-world-f77b4f6c8 to 1",
				"2: replicaset-controller ReplicaSet.SuccessfulCreate (1) Created pod: hello-world-f77b4f6c8-6tcj2",
				"3: default-scheduler Pod.Scheduled (2) Successfully assigned default/hello-world-f77b4f6c8-6tcj2 to node2",
				"4: kubelet Pod.Pulling (2) Pulling image \"nginx:1.19.3-alpine\"",
				"5: kubelet Pod.Pulled (2) Successfully pulled image \"nginx:1.19.3-alpine\"",
				"6: kubelet Pod.Created (2) Created container hello-world",
				"7: kubelet Pod.Started (2) Started container hello-world",
				"8: deployment-controller Deployment.ScalingReplicaSet (0) Scaled down replica set hello-world-779cbf9f67 to 0",
				"9: replicaset-controller ReplicaSet.SuccessfulDelete (8) Deleted pod: hello-world-779cbf9f67-nbwfm",
			},
		},
	}
	threshold, err := time.Parse(time.RFC3339, fluxDeploymentUpdateEventsThresholdStr)
	g.Expect(err).NotTo(o.HaveOccurred())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, r, exporter, _ := newTestEventWatcher(&deploy1, &rs1, &rs2, &pod1)
			defer r.stop()
			for _, index := range tt.perm {
				var event corev1.Event
				mustParse(t, fluxDeploymentUpdateEvents[index], &event)
				g.Expect(r.handleEvent(ctx, &event)).To(o.Succeed())
			}
			g.Expect(r.checkOlderPending(ctx, threshold)).To(o.Succeed())
			r.flushOutgoing(ctx, threshold)
			g.Expect(exporter.dump()).To(o.Equal(tt.wantTraces))
		})
	}
}

func TestStsRolloutFromFlux(t *testing.T) {
	g := o.NewWithT(t)

	var (
		sts1       unstructured.Unstructured
		pod2, pod3 unstructured.Unstructured
	)
	mustParse(t, sts1str, &sts1)
	mustParse(t, stsPod2str, &pod2)
	mustParse(t, stsPod3str, &pod3)

	tests := []struct {
		name       string
		perm       []int
		wantTraces []string
	}{
		{
			name: "flux-sts",
			perm: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
			wantTraces: []string{
				"0: flux StatefulSet.Sync Commit fc4e825b46ac: Update ingester to latest, in dev",
				"1: statefulset-controller StatefulSet.SuccessfulDelete (0) delete Pod ingester-3 in StatefulSet ingester successful",
				"2: kubelet Pod.Killing (1) Stopping container ingester",
				"3: statefulset-controller StatefulSet.SuccessfulCreate (0) create Pod ingester-3 in StatefulSet ingester successful",
				"4: default-scheduler Pod.Scheduled (3) Successfully assigned cortex/ingester-3 to ip-172-20-2-25.ec2.internal",
				"5: kubelet Pod.Pulled (3) Container image \"cortexproject/cortex:master-a2be3d8\" already present on machine",
				"6: kubelet Pod.Created (3) Created container ingester",
				"7: kubelet Pod.Started (3) Started container ingester",
				"8: kubelet Pod.Killing (0) Stopping container ingester",
				"9: statefulset-controller StatefulSet.SuccessfulDelete (0) delete Pod ingester-2 in StatefulSet ingester successful",
				"10: default-scheduler Pod.Scheduled (9) Successfully assigned cortex/ingester-2 to ip-172-20-2-22.ec2.internal",
				"11: statefulset-controller StatefulSet.SuccessfulCreate (0) create Pod ingester-2 in StatefulSet ingester successful",
				"12: kubelet Pod.Pulled (11) Container image \"cortexproject/cortex:master-a2be3d8\" already present on machine",
				"13: kubelet Pod.Created (11) Created container ingester",
				"14: kubelet Pod.Started (11) Started container ingester",
			},
		},
	}
	threshold, err := time.Parse(time.RFC3339, stsUpdateEventsThresholdStr)
	g.Expect(err).NotTo(o.HaveOccurred())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, r, exporter, _ := newTestEventWatcher(&sts1, &pod2, &pod3)
			defer r.stop()
			for _, index := range tt.perm {
				var event corev1.Event
				mustParse(t, stsUpdateEvents[index], &event)
				g.Expect(r.handleEvent(ctx, &event)).To(o.Succeed())
			}
			g.Expect(r.checkOlderPending(ctx, threshold)).To(o.Succeed())
			r.flushOutgoing(ctx, threshold)
			g.Expect(exporter.dump()).To(o.Equal(tt.wantTraces))
		})
	}
}

func TestDeployWithConditions(t *testing.T) {
	g := o.NewWithT(t)

	tests := []struct {
		name       string
		filename   string
		wantTraces []string
	}{
		{
			name:     "test-playback",
			filename: "testdata/deployment-2-pods.yaml",
			wantTraces: []string{
				"0: kubectl-client-side-apply Deployment.Update ",
				"1: kube-controller-manager Deployment.MinimumReplicasUnavailable (0) Deployment does not have minimum availability.",
				// Want next two to be parented off 0
				"2: kube-controller-manager Deployment.ReplicaSetUpdated (1) ReplicaSet \"px-5f87d8856c\" is progressing.",
				"3: kube-controller-manager Deployment.MinimumReplicasAvailable (2) Deployment has minimum availability.",
				"4: deployment-controller Deployment.ScalingReplicaSet (0) Scaled up replica set px-5f87d8856c to 2",
				"5: replicaset-controller ReplicaSet.SuccessfulCreate (4) Created pod: px-5f87d8856c-8f6bk",
				// why is next one 'unknown' not kubelet as at 18?
				"6: unknown Pod.PodScheduled (5) PodScheduled True",
				"7: default-scheduler Pod.Scheduled (5) Successfully assigned default/px-5f87d8856c-8f6bk to kind-control-plane",
				"8: kubelet Pod.Pulling (5) Pulling image \"ghcr.io/stefanprodan/podinfo:5.1.1\"",
				"9: kubelet Pod.Pulled (5) Successfully pulled image \"ghcr.io/stefanprodan/podinfo:5.1.1\" in 6.055782903s",
				"10: kubelet Pod.Created (5) Created container podinfo",
				"11: kubelet Pod.Started (5) Started container podinfo",
				"12: kubelet Pod.Ready (5) Ready True",
				"13: kubelet Pod.ContainersReady (5) ContainersReady True",
				"14: replicaset-controller ReplicaSet.SuccessfulCreate (4) Created pod: px-5f87d8856c-c29hj",
				"15: kubelet Pod.Initialized (14) Initialized True",
				"16: kubelet Pod.ContainersNotReady (14) containers with unready status: [podinfo]",
				"17: kubelet Pod.ContainersNotReady (14) containers with unready status: [podinfo]",
				"18: kubelet Pod.PodScheduled (14) PodScheduled True",
				"19: default-scheduler Pod.Scheduled (14) Successfully assigned default/px-5f87d8856c-c29hj to kind-control-plane",
				"20: kubelet Pod.Pulling (14) Pulling image \"ghcr.io/stefanprodan/podinfo:5.1.1\"",
				"21: kubelet Pod.Pulled (14) Successfully pulled image \"ghcr.io/stefanprodan/podinfo:5.1.1\" in 5.510339507s",
				"22: kubelet Pod.Created (14) Created container podinfo",
				"23: kubelet Pod.Started (14) Started container podinfo",
				"24: kubelet Pod.Ready (14) Ready True",
				"25: kubelet Pod.ContainersReady (14) ContainersReady True",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs, maxTimestamp, err := getInitialObjects(tt.filename)
			g.Expect(err).NotTo(o.HaveOccurred())
			ctx, r, exporter, _ := newTestEventWatcher(objs...)
			defer r.stop()
			playback(ctx, r, tt.filename)
			threshold := maxTimestamp.Add(time.Second * 10)
			g.Expect(r.checkOlderPending(ctx, threshold)).To(o.Succeed())
			r.flushOutgoing(ctx, threshold)
			g.Expect(exporter.dump()).To(o.Equal(tt.wantTraces))
		})
	}
}
