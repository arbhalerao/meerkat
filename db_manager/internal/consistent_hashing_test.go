package internal

import (
	"fmt"
	"math"
	"testing"
)

func TestNewConsistentHasher(t *testing.T) {
	h := NewConsistentHasher()
	if h.Size() != 0 {
		t.Fatalf("expected empty hasher, got size %d", h.Size())
	}
	nodes := h.GetNodes()
	if len(nodes) != 0 {
		t.Fatalf("expected no nodes, got %d", len(nodes))
	}
}

func TestAddNode(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	if h.Size() != 1 {
		t.Fatalf("expected 1 node, got %d", h.Size())
	}

	h.AddNode("server-1")
	if h.Size() != 1 {
		t.Fatalf("expected 1 node after duplicate add, got %d", h.Size())
	}

	h.AddNode("server-2")
	if h.Size() != 2 {
		t.Fatalf("expected 2 nodes, got %d", h.Size())
	}
}

func TestRemoveNode(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")

	h.RemoveNode("server-1")
	if h.Size() != 1 {
		t.Fatalf("expected 1 node after removal, got %d", h.Size())
	}

	h.RemoveNode("server-99")
	if h.Size() != 1 {
		t.Fatalf("expected 1 node after no-op removal, got %d", h.Size())
	}

	h.RemoveNode("server-2")
	if h.Size() != 0 {
		t.Fatalf("expected 0 nodes, got %d", h.Size())
	}
}

func TestGetNode_EmptyRing(t *testing.T) {
	h := NewConsistentHasher()
	_, ok := h.GetNode("any-key")
	if ok {
		t.Fatal("expected false for empty ring")
	}
}

func TestGetNode_SingleNode(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")

	node, ok := h.GetNode("key1")
	if !ok || node != "server-1" {
		t.Fatalf("expected server-1, got %s (ok=%v)", node, ok)
	}

	for i := 0; i < 100; i++ {
		node, ok := h.GetNode(fmt.Sprintf("key-%d", i))
		if !ok || node != "server-1" {
			t.Fatalf("key-%d: expected server-1, got %s", i, node)
		}
	}
}

func TestGetNode_Consistency(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")
	h.AddNode("server-3")

	node1, _ := h.GetNode("user:123")
	for i := 0; i < 100; i++ {
		node, _ := h.GetNode("user:123")
		if node != node1 {
			t.Fatalf("inconsistent mapping: expected %s, got %s on iteration %d", node1, node, i)
		}
	}
}

func TestGetNode_Distribution(t *testing.T) {
	h := NewConsistentHasher()
	nodes := []string{"server-1", "server-2", "server-3"}
	for _, n := range nodes {
		h.AddNode(n)
	}

	counts := make(map[string]int)
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		node, ok := h.GetNode(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("unexpected empty result")
		}
		counts[node]++
	}

	for _, n := range nodes {
		if counts[n] == 0 {
			t.Errorf("node %s got zero keys — poor distribution", n)
		}
	}

	for n, c := range counts {
		pct := float64(c) / float64(numKeys) * 100
		if pct > 80 {
			t.Errorf("node %s has %.1f%% of keys — imbalanced", n, pct)
		}
	}
}

func TestGetNode_MinimalRemapping(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")

	mappings := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := h.GetNode(key)
		mappings[key] = node
	}

	h.AddNode("server-3")
	changed := 0
	for key, oldNode := range mappings {
		newNode, _ := h.GetNode(key)
		if newNode != oldNode {
			changed++
		}
	}

	maxAllowed := int(math.Ceil(float64(len(mappings)) * 0.6))
	if changed > maxAllowed {
		t.Errorf("too many keys remapped: %d out of %d (max expected %d)", changed, len(mappings), maxAllowed)
	}
}

func TestGetReplicaNodes(t *testing.T) {
	h := NewConsistentHasher()

	nodes := h.GetReplicaNodes("key", 2)
	if len(nodes) != 0 {
		t.Fatalf("expected nil for empty ring, got %v", nodes)
	}

	h.AddNode("server-1")

	nodes = h.GetReplicaNodes("key", 2)
	if len(nodes) != 1 || nodes[0] != "server-1" {
		t.Fatalf("expected [server-1], got %v", nodes)
	}

	h.AddNode("server-2")
	h.AddNode("server-3")

	nodes = h.GetReplicaNodes("key", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 replica nodes, got %d", len(nodes))
	}

	if nodes[0] == nodes[1] {
		t.Fatalf("replica nodes should be distinct, got %v", nodes)
	}

	primary, _ := h.GetNode("key")
	if nodes[0] != primary {
		t.Fatalf("first replica should be primary: expected %s, got %s", primary, nodes[0])
	}
}

func TestGetReplicaNodes_AllNodes(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")
	h.AddNode("server-3")

	nodes := h.GetReplicaNodes("key", 5)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes (capped), got %d", len(nodes))
	}

	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node %s in replicas", n)
		}
		seen[n] = true
	}
}

func TestGetReplicaNodes_OwnershipShiftOnAdd(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")

	type ownership struct {
		primary  string
		replicas []string
	}
	before := make(map[string]ownership)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		replicas := h.GetReplicaNodes(key, 2)
		before[key] = ownership{primary: replicas[0], replicas: replicas}
	}

	h.AddNode("server-3")

	shifted := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		newReplicas := h.GetReplicaNodes(key, 2)
		if newReplicas[0] != before[key].primary {
			shifted++
		}
	}

	if shifted == 0 {
		t.Error("no keys shifted ownership after adding server-3 — migration would have nothing to do")
	}
}

func TestGetReplicaNodes_OwnershipShiftOnRemove(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")
	h.AddNode("server-3")

	keysOnServer2 := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		primary, _ := h.GetNode(key)
		if primary == "server-2" {
			keysOnServer2++
		}
	}

	h.RemoveNode("server-2")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		primary, ok := h.GetNode(key)
		if !ok {
			t.Fatalf("GetNode failed after removal for key %s", key)
		}
		if primary == "server-2" {
			t.Fatalf("key %s still maps to removed server-2", key)
		}
	}

	if keysOnServer2 == 0 {
		t.Error("server-2 had no keys before removal — test is not meaningful")
	}
}

func TestReconcile(t *testing.T) {
	h := NewConsistentHasher()
	h.AddNode("server-1")
	h.AddNode("server-2")
	h.AddNode("server-3")

	h.Reconcile([]string{"server-1", "server-3"})
	if h.Size() != 2 {
		t.Fatalf("expected 2 nodes after reconcile, got %d", h.Size())
	}

	nodes := h.GetNodes()
	nodeSet := make(map[string]bool)
	for _, n := range nodes {
		nodeSet[n] = true
	}
	if nodeSet["server-2"] {
		t.Fatal("server-2 should have been removed by reconcile")
	}

	h.Reconcile([]string{"server-1", "server-3", "server-4"})
	if h.Size() != 3 {
		t.Fatalf("expected 3 nodes after reconcile, got %d", h.Size())
	}
}
