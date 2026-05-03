package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

type OAuthTokenHandler interface {
	SetOAuthBearerToken(oauthBearerToken ckafka.OAuthBearerToken) error
	SetOAuthBearerTokenFailure(errstr string) error
}

func NewOAuthProvider(saslAlgorithm string, brokers []string, opts OAuthProviderOpts) (OAuthTokenProvider, error) {
	if saslAlgorithm == saslAzureEntra {
		return newAzureEntraOAuthTokenProvider(brokers, opts.azureTokenCredential)
	}

	return nil, NewXk6KafkaError(failedCreateOAuthTokenProvider, saslAlgorithm+" is not a supported OAuth Provider.", nil)
}

type AzureEntraOAuthTokenProvider struct {
	tokenCredential azcore.TokenCredential
	requestOpts     policy.TokenRequestOptions
}

func newAzureEntraOAuthTokenProvider(
	brokers []string, tokenCredential azcore.TokenCredential,
) (*AzureEntraOAuthTokenProvider, error) {
	var cred azcore.TokenCredential
	var err error

	if len(brokers) != 1 {
		return nil, NewXk6KafkaError(
			failedGetOAuthToken,
			fmt.Sprintf("Azure Entra OAuth requires a single bootstrap server, but %d were provided.", len(brokers)),
			nil,
		)
	}

	host, _, err := net.SplitHostPort(brokers[0])
	if err != nil {
		return nil, NewXk6KafkaError(failedGetOAuthToken, "Azure Entra OAuth requires a valid host:port for the broker.", err)
	}

	scope := fmt.Sprintf("https://%s/.default", host)

	if tokenCredential == nil {
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, NewXk6KafkaError(
				failedCreateOAuthTokenProvider,
				"Failed to create Azure Entra OAuth Token Provider.",
				err,
			)
		}
	} else {
		cred = tokenCredential
	}

	return &AzureEntraOAuthTokenProvider{
		tokenCredential: cred,
		requestOpts: policy.TokenRequestOptions{
			Scopes: []string{scope},
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
		return OAuthToken{}, NewXk6KafkaError(
			failedGetOAuthToken,
			"Azure Entra OAuth token is missing the subject claim.",
			err,
		)
	}

	return OAuthToken{
		Subject:   subject,
		Token:     token.Token,
		ExpiresOn: token.ExpiresOn,
		RefreshOn: token.RefreshOn,
	}, nil
}

func refreshOAuthToken(ctx context.Context, saslContext SASLContext, handler OAuthTokenHandler) error {
	if saslContext.OAuthProvider == nil {
		return NewXk6KafkaError(
			failedGetOAuthToken,
			"Failed to get an OAuth token. The OAuth provider is nil in the SASL context.",
			nil,
		)
	}

	oauthProvider := *saslContext.OAuthProvider

	token, err := oauthProvider.GetToken(ctx)
	if err == nil {
		err = handler.SetOAuthBearerToken(ckafka.OAuthBearerToken{
			TokenValue: token.Token,
			Expiration: token.ExpiresOn,
			Principal:  token.Subject,
			Extensions: make(map[string]string),
		})
		if err != nil {
			return NewXk6KafkaError(
				failedGetOAuthToken,
				"Failed to set an OAuth token. The Kafka client rejected the OAuth token.",
				err,
			)
		}
	} else {
		err = handler.SetOAuthBearerTokenFailure(err.Error())
		if err != nil {
			return NewXk6KafkaError(
				failedGetOAuthToken,
				"Failed to set an OAuth token error. The Kafka client rejected the OAuth token error.",
				err,
			)
		}
	}
	return nil
}
