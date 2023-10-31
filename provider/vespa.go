package provider

import (
    "context"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "net/url"

  
    pc "github.com/featureform/provider/provider_config"
   pt "github.com/featureform/provider/provider_type"
)

type VespaVectorStore struct {
    url string
}

type VectorStore interface {
 CreateIndex(feature, variant string, vectorType VectorType) (VectorStoreTable, error)
 DeleteIndex(feature, variant string) error
 OnlineStore
}

type VectorStoreTable interface {
 OnlineStoreTable
 Nearest(feature, variant string, vector []float32, k int32) ([]string, error)
}
func (v *VespaVectorStore) GetNearestNeighbors(ctx context.Context, featureVector []float32, k int) ([]vector.Neighbor, error) {
    payload := struct {
        FeatureVector []float32 json:"feature_vector"
        K             int       json:"k"
    }{
        FeatureVector: featureVector,
        K:             k,
    }

    jsonPayload, err := json.Marshal(payload)
    if err != nil {
        return nil, err
    }

    req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/nearest_neighbors", v.url), bytes.NewReader(jsonPayload))
    if err != nil {
        return nil, err
    }

    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }

    defer resp.Body.Close()

    respBody, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("Vespa API returned status code %d: %s", resp.StatusCode, string(respBody))
    }

    var neighbors []vector.Neighbor
    err = json.Unmarshal(respBody, &neighbors)
    if err != nil {
        return nil, err
    }

    return neighbors, nil
}

func (v *VespaVectorStore) PutEmbeddings(ctx context.Context, embeddings map[string][]float32) error {
    payload := struct {
        Embeddings map[string][]float32 json:"embeddings"
    }{
        Embeddings: embeddings,
    }

    jsonPayload, err := json.Marshal(payload)
    if err != nil {
        return err
    }

    req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/embeddings", v.url), bytes.NewReader(jsonPayload))
    if err != nil {
        return err
    }

    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }

    defer resp.Body.Close()

    respBody, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return err
    }

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("Vespa API returned status code %d: %s", resp.StatusCode, string(respBody))
    }

    return nil
}

func main() {
    // Create a new VespaVectorStore instance
    vectorStore := VespaVectorStore{url: "http://localhost:8080"}

    // Register the VespaVectorStore instance with Featureform
    vector.RegisterVectorStore(&vectorStore)

    // Get the nearest neighbors for a given feature vector
    nearestNeighbors, err := vector.GetNearestNeighbors([]float32{1.0, 2.0, 3.0}, 10)
    if err != nil {
        fmt.Println(err)
        return
    }

    // Print the IDs of the nearest neighbors
    for _, neighbor := range nearestNeighbors {
        fmt.Println(neighbor.ID)
    }
}
