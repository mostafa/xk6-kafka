package kafka

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type OAuthTokenRequest struct {
	BootstrapServers []string
}

type OAuthToken struct {
	Token     string
	ExpiresOn time.Time
	RefreshOn time.Time
}

type OAuthTokenProvider interface {
	GetToken(ctx context.Context, req OAuthTokenRequest) (OAuthToken, error)
}

type OAuthProviderOpts struct {
	azureTokenCredential azcore.TokenCredential
}

func NewOAuthProvider(saslAlgorithm string, opts OAuthProviderOpts) (OAuthTokenProvider, error) {
	switch saslAlgorithm {
	case saslAzureEntra:
		return newAzureEntraOAuthTokenProvider(opts.azureTokenCredential)
	}

	return nil, nil
}

type AzureEntraOAuthTokenProvider struct {
	tokenCredential azcore.TokenCredential
}

func newAzureEntraOAuthTokenProvider(tokenCredential azcore.TokenCredential) (*AzureEntraOAuthTokenProvider, error) {
	var cred azcore.TokenCredential
	var err error

	if tokenCredential == nil {
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, NewXk6KafkaError(
				failedCreateOAuthTokenProvider,
				"Failed to create Azure Entra OAuth Token Provider",
				err,
			)
		}
	} else {
		cred = tokenCredential
	}

	return &AzureEntraOAuthTokenProvider{
		cred,
	}, nil
}

func (a *AzureEntraOAuthTokenProvider) GetToken(ctx context.Context, req OAuthTokenRequest) (OAuthToken, error) {
	if len(req.BootstrapServers) != 1 {
		return OAuthToken{}, NewXk6KafkaError(failedGetOAuthToken, fmt.Sprintf("Azure Entra OAuth requires a single bootstrap server, but %d were provided.", len(req.BootstrapServers)), nil)
	}

	brokerUrl := url.URL{
		Scheme: "https",
		Host:   req.BootstrapServers[0],
	}

	opts := policy.TokenRequestOptions{
		Scopes: []string{brokerUrl.String()},
	}

	token, err := a.tokenCredential.GetToken(ctx, opts)
	if err != nil {
		return OAuthToken{}, NewXk6KafkaError(failedGetOAuthToken, "Azure Entra OAuth token could not be retrieved.", err)
	}

	return OAuthToken{
		Token:     token.Token,
		ExpiresOn: token.ExpiresOn,
		RefreshOn: token.RefreshOn,
	}, nil
}
