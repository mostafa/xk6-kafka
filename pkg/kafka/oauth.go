package kafka

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/golang-jwt/jwt/v5"
)

type OAuthToken struct {
	Token     string
	Subject   string
	ExpiresOn time.Time
	RefreshOn time.Time
}

type OAuthTokenProvider interface {
	GetToken(ctx context.Context) (OAuthToken, error)
}

type OAuthProviderOpts struct {
	azureTokenCredential azcore.TokenCredential
}

func NewOAuthProvider(saslAlgorithm string, brokers []string, opts OAuthProviderOpts) (OAuthTokenProvider, error) {
	switch saslAlgorithm {
	case saslAzureEntra:
		return newAzureEntraOAuthTokenProvider(brokers, opts.azureTokenCredential)
	}

	return nil, nil
}

type AzureEntraOAuthTokenProvider struct {
	tokenCredential azcore.TokenCredential
	requestOpts     policy.TokenRequestOptions
}

func newAzureEntraOAuthTokenProvider(brokers []string, tokenCredential azcore.TokenCredential) (*AzureEntraOAuthTokenProvider, error) {
	var cred azcore.TokenCredential
	var err error

	if len(brokers) != 1 {
		return nil, NewXk6KafkaError(failedGetOAuthToken, fmt.Sprintf("Azure Entra OAuth requires a single bootstrap server, but %d were provided.", len(brokers)), nil)
	}

	brokerUrl := url.URL{
		Scheme: "https",
		Host:   brokers[0],
	}

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
		tokenCredential: cred,
		requestOpts: policy.TokenRequestOptions{
			Scopes: []string{brokerUrl.String()},
		},
	}, nil
}

func (a *AzureEntraOAuthTokenProvider) GetToken(ctx context.Context) (OAuthToken, error) {
	token, err := a.tokenCredential.GetToken(ctx, a.requestOpts)
	if err != nil {
		return OAuthToken{}, NewXk6KafkaError(failedGetOAuthToken, "Azure Entra OAuth token could not be retrieved.", err)
	}

	parsedToken, _, err := jwt.NewParser().ParseUnverified(token.Token, jwt.MapClaims{})
	if err != nil {
		return OAuthToken{}, NewXk6KafkaError(failedGetOAuthToken, "Failed to parse Azure Entra OAuth token.", err)
	}

	claims, ok := parsedToken.Claims.(jwt.MapClaims)
	if !ok {
		return OAuthToken{}, NewXk6KafkaError(failedGetOAuthToken, "Azure Entra OAuth token contained invalid claims.", err)
	}

	subject, err := claims.GetSubject()
	if err != nil {
		return OAuthToken{}, NewXk6KafkaError(failedGetOAuthToken, "Azure Entra OAuth token is missing the subject claim.", err)
	}

	return OAuthToken{
		Subject:   subject,
		Token:     token.Token,
		ExpiresOn: token.ExpiresOn,
		RefreshOn: token.RefreshOn,
	}, nil
}
