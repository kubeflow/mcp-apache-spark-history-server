//go:build aws

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
)

const (
	// Default endpoint pattern for the SageMaker Unified Studio MCP service.
	// The region is interpolated at runtime.
	defaultEndpointPattern = "https://sagemaker-unified-studio-mcp.%s.api.aws"
	sigV4Service           = "sagemaker-unified-studio-mcp"
	httpTimeout            = 180 * time.Second
)

type mcpRequest struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int       `json:"id"`
	Method  string    `json:"method"`
	Params  mcpParams `json:"params"`
}

type mcpParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func runTroubleshoot(cfg *config.AwsTroubleshooting, platformType string, platformParams map[string]string) error {
	ctx := context.Background()

	// Use the default AWS credential chain (env vars, shared credentials, IAM roles, etc.)
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	fmt.Println("Analyzing Spark workload...")
	analysisResult, err := callMCPTool(ctx, awsCfg, cfg.Region, "spark-troubleshooting", "analyze_spark_workload", map[string]any{
		"platform_type":   platformType,
		"platform_params": platformParams,
	})
	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	analysisJSON, _ := json.MarshalIndent(analysisResult, "", "  ")
	fmt.Printf("\nAnalysis Result:\n%s\n", analysisJSON)

	// Automatically chain to code recommendation if the analysis suggests it
	if nextAction, ok := analysisResult["next_action"]; ok {
		if actionStr, ok := nextAction.(string); ok && strings.Contains(actionStr, "spark_code_recommendation") {
			fmt.Println("\nGetting code recommendations...")
			codeResult, err := callMCPTool(ctx, awsCfg, cfg.Region, "spark-code-recommendation", "spark_code_recommendation", map[string]any{
				"platform_type":   platformType,
				"platform_params": platformParams,
			})
			if err != nil {
				return fmt.Errorf("code recommendation failed: %w", err)
			}
			codeJSON, _ := json.MarshalIndent(codeResult, "", "  ")
			fmt.Printf("\nCode Recommendation:\n%s\n", codeJSON)
		}
	}

	return nil
}

func callMCPTool(ctx context.Context, awsCfg aws.Config, region, serverPath, toolName string, arguments map[string]any) (map[string]any, error) {
	endpoint := fmt.Sprintf(defaultEndpointPattern+"/%s/mcp", region, serverPath)

	reqBody := mcpRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params: mcpParams{
			Name:      toolName,
			Arguments: arguments,
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshalling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	// Sign with SigV4
	creds, err := awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving credentials: %w", err)
	}

	hash := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(hash[:])

	signer := v4.NewSigner()
	err = signer.SignHTTP(ctx, creds, req, payloadHash, sigV4Service, region, time.Now())
	if err != nil {
		return nil, fmt.Errorf("signing request: %w", err)
	}

	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("endpoint returned %d: %s", resp.StatusCode, string(respBody))
	}

	var mcpResp struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(respBody, &mcpResp); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	if mcpResp.Error != nil {
		return nil, fmt.Errorf("remote error: %s", mcpResp.Error.Message)
	}

	if len(mcpResp.Result.Content) == 0 {
		return nil, fmt.Errorf("empty response from endpoint")
	}

	var result map[string]any
	if err := json.Unmarshal([]byte(mcpResp.Result.Content[0].Text), &result); err != nil {
		return nil, fmt.Errorf("parsing tool result: %w", err)
	}

	return result, nil
}
