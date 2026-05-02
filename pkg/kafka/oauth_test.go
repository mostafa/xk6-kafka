package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

var testSigningKey = []byte("test-signing-key-unit-test-only")

type testTokenCredential struct {
	Subject string
	Error   error
}

func (t *testTokenCredential) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if t.Error != nil {
		return azcore.AccessToken{}, t.Error
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": t.Subject,
	})

	tokenStr, err := token.SignedString(testSigningKey)
	if err != nil {
		return azcore.AccessToken{}, err
	}

	return azcore.AccessToken{Token: tokenStr, ExpiresOn: time.Now().Add(24 * time.Hour)}, nil
}

func TestGetOAuthTokenSuccess(t *testing.T) {
	testCases := map[string]struct {
		saslAlgorithm        string
		opts                 OAuthProviderOpts
		expectedSubject      string
		expectedExpiredAfter time.Time
		expectedRefreshAfter time.Time
	}{
		"azure entra": {
			saslAlgorithm: saslAzureEntra,
			opts: OAuthProviderOpts{
				azureTokenCredential: &testTokenCredential{
					Subject: "test-sub",
				},
			},
			expectedSubject:      "test-sub",
			expectedExpiredAfter: time.Now(),
			expectedRefreshAfter: time.Time{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			provider, err := NewOAuthProvider(test.saslAlgorithm, []string{"broker1"}, test.opts)
			require.NoError(t, err)

			token, err := provider.GetToken(t.Context())

			require.NoError(t, err)
			require.NotEmpty(t, token.Token)
			require.Equal(t, test.expectedSubject, token.Subject)
			require.GreaterOrEqual(t, token.ExpiresOn, test.expectedExpiredAfter)
			require.GreaterOrEqual(t, token.RefreshOn, test.expectedRefreshAfter)
		})
	}
}

func TestGetOAuthTokenFailure(t *testing.T) {
	testCases := map[string]struct {
		saslAlgorithm string
		opts          OAuthProviderOpts
	}{
		"azure entra": {
			saslAlgorithm: saslAzureEntra,
			opts: OAuthProviderOpts{
				azureTokenCredential: &testTokenCredential{
					Error: errors.New("failed to retrieve token"),
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			provider, err := NewOAuthProvider(test.saslAlgorithm, []string{"broker1"}, test.opts)
			require.NoError(t, err)

			_, err = provider.GetToken(t.Context())

			xk6KafkaError, ok := err.(*Xk6KafkaError)
			require.True(t, ok, "error is not Xk6KafkaError")

			require.Equal(t, failedGetOAuthToken, xk6KafkaError.Code)
		})
	}
}
