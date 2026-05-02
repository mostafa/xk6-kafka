package kafka

import (
	"errors"
	"testing"
	"time"

	azcoreFake "github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/stretchr/testify/require"
)

func TestGetOAuthTokenSuccess(t *testing.T) {
	testCases := map[string]struct {
		saslAlgorithm        string
		opts                 OAuthProviderOpts
		expectedToken        string
		expectedExpiredAfter time.Time
		expectedRefreshAfter time.Time
	}{
		"azure entra": {
			saslAlgorithm: saslAzureEntra,
			opts: OAuthProviderOpts{
				azureTokenCredential: &azcoreFake.TokenCredential{},
			},
			expectedToken:        "fake_token",
			expectedExpiredAfter: time.Now(),
			expectedRefreshAfter: time.Time{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			provider, err := NewOAuthProvider(test.saslAlgorithm, test.opts)
			require.NoError(t, err)

			token, err := provider.GetToken(t.Context(), OAuthTokenRequest{
				BootstrapServers: []string{"broker1"},
			})

			require.NoError(t, err)
			require.Equal(t, test.expectedToken, token.Token)
			require.GreaterOrEqual(t, token.ExpiresOn, test.expectedExpiredAfter)
			require.GreaterOrEqual(t, token.RefreshOn, test.expectedRefreshAfter)
		})
	}
}

func TestGetOAuthTokenFailure(t *testing.T) {
	testCases := map[string]struct {
		saslAlgorithm string
		opts          OAuthProviderOpts
		errorCallback func(opts OAuthProviderOpts)
	}{
		"azure entra": {
			saslAlgorithm: saslAzureEntra,
			opts: OAuthProviderOpts{
				azureTokenCredential: &azcoreFake.TokenCredential{},
			},
			errorCallback: func(opts OAuthProviderOpts) {
				fakeCred := opts.azureTokenCredential.(*azcoreFake.TokenCredential)
				fakeCred.SetError(errors.New("failed to retrieve token"))
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			test.errorCallback(test.opts)

			provider, err := NewOAuthProvider(test.saslAlgorithm, test.opts)
			require.NoError(t, err)

			_, err = provider.GetToken(t.Context(), OAuthTokenRequest{
				BootstrapServers: []string{"broker1"},
			})

			xk6KafkaError, ok := err.(*Xk6KafkaError)
			require.True(t, ok, "error is not Xk6KafkaError")

			require.Equal(t, failedGetOAuthToken, xk6KafkaError.Code)
		})
	}
}
