package c3

import (
	"math"
	"math/rand"
	"strconv"
)

// MinimumSpanningTree generates a minimum spanning tree for a fully connected graph with `totalNodes` nodes
func MinimumSpanningTree(totalNodes int) map[string][]string {
	// Generate node names
	nodes := make([]string, totalNodes)
	for i := 0; i < totalNodes; i++ {
		nodes[i] = "n" + strconv.Itoa(i)
	}

	// Initialize adjacency matrix with random weights for the fully connected graph
	adjMatrix := make([][]int, totalNodes)
	for i := range adjMatrix {
		adjMatrix[i] = make([]int, totalNodes)
		for j := range adjMatrix[i] {
			if i != j {
				adjMatrix[i][j] = rand.Intn(100) + 1 // Random weight between 1 and 100
			} else {
				adjMatrix[i][j] = 0 // No self-loops
			}
		}
	}

	// Implementing Prim's algorithm
	inMST := make([]bool, totalNodes)
	key := make([]int, totalNodes)
	parent := make([]int, totalNodes)

	for i := 0; i < totalNodes; i++ {
		key[i] = math.MaxInt
		parent[i] = -1
	}

	// Start from the first node
	key[0] = 0
	for count := 0; count < totalNodes-1; count++ {
		u := minKey(key, inMST, totalNodes)
		inMST[u] = true

		for v := 0; v < totalNodes; v++ {
			if adjMatrix[u][v] != 0 && !inMST[v] && adjMatrix[u][v] < key[v] {
				parent[v] = u
				key[v] = adjMatrix[u][v]
			}
		}
	}

	// Construct the result map
	result := make(map[string][]string)
	for i := 1; i < totalNodes; i++ {
		src := nodes[parent[i]]
		dest := nodes[i]
		result[src] = append(result[src], dest)
		result[dest] = append(result[dest], src) // Since it's an undirected graph
	}

	return result
}

// minKey finds the vertex with the minimum key value that is not yet included in MST
func minKey(key []int, inMST []bool, totalNodes int) int {
	min := math.MaxInt
	minIndex := -1

	for v := 0; v < totalNodes; v++ {
		if !inMST[v] && key[v] < min {
			min = key[v]
			minIndex = v
		}
	}

	return minIndex
}
