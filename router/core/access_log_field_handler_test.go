package core

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router/internal/expr"
	"github.com/wundergraph/cosmo/router/internal/requestlogger"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/operationreport"
	"go.uber.org/zap"
)

func TestAccessLogsFieldHandler(t *testing.T) {
	t.Parallel()

	t.Run("run without any expressions", func(t *testing.T) {
		t.Parallel()

		logger := &zap.Logger{}

		req, err := http.NewRequest(http.MethodPost, "http://localhost:3002/graphql", nil)
		require.NoError(t, err)
		rcc := buildRequestContext(requestContextOptions{r: req})
		req = req.WithContext(withRequestContext(req.Context(), rcc))

		response := RouterAccessLogsFieldHandler(
			logger,
			make([]config.CustomAttribute, 0),
			make([]requestlogger.ExpressionAttribute, 0),
			nil,
			req,
			nil,
			nil,
		)

		require.Len(t, response, 1)
	})

	t.Run("run expression without error", func(t *testing.T) {
		t.Parallel()

		logger := &zap.Logger{}

		req, err := http.NewRequest(http.MethodPost, "http://localhost:3002/graphql", nil)

		require.NoError(t, err)
		rcc := buildRequestContext(requestContextOptions{r: req})
		req = req.WithContext(withRequestContext(req.Context(), rcc))

		manager := expr.CreateNewExprManager()
		expressionResponseKey := "testkey"
		expression, err := manager.CompileAnyExpression("request.error ?? request.url")
		require.NoError(t, err)

		exprAttributes := []requestlogger.ExpressionAttribute{
			{
				Key:     expressionResponseKey,
				Default: "somedefaultvalue",
				Expr:    expression,
			},
		}

		response := RouterAccessLogsFieldHandler(
			logger,
			make([]config.CustomAttribute, 0),
			exprAttributes,
			nil,
			req,
			nil,
			nil,
		)

		expressionResponse := response[1]
		require.Equal(t, expressionResponse.Key, expressionResponseKey)
		require.Equal(t, expressionResponse.Interface, rcc.expressionContext.Request.URL)
	})

	t.Run("run expression with an error", func(t *testing.T) {
		t.Parallel()

		logger := &zap.Logger{}

		req, err := http.NewRequest(http.MethodPost, "http://localhost:3002/graphql", nil)
		require.NoError(t, err)
		rcc := buildRequestContext(requestContextOptions{r: req})

		requestError := &reportError{
			report: &operationreport.Report{
				InternalErrors: []error{
					errors.New("new error"),
				},
				ExternalErrors: nil,
			},
		}
		rcc.SetError(requestError)

		req = req.WithContext(withRequestContext(req.Context(), rcc))

		manager := expr.CreateNewExprManager()
		expression, err := manager.CompileAnyExpression("request.error ?? request.url")
		require.NoError(t, err)
		expressionResponseKey := "testkey"

		exprAttributes := []requestlogger.ExpressionAttribute{
			{
				Key:     expressionResponseKey,
				Default: "somedefaultvalue",
				Expr:    expression,
			},
		}

		response := RouterAccessLogsFieldHandler(
			logger,
			make([]config.CustomAttribute, 0),
			exprAttributes,
			nil,
			req,
			nil,
			nil,
		)

		expressionResponse := response[1]
		require.IsType(t, &ExprWrapError{}, expressionResponse.Interface)
		require.Equal(t, expressionResponseKey, expressionResponse.Key)
		require.Equal(t, &ExprWrapError{requestError}, expressionResponse.Interface)
	})

	t.Run("expression failing at runtime falls back to the configured default", func(t *testing.T) {
		t.Parallel()

		logger := zap.NewNop()

		manager := expr.CreateNewExprManager()
		// Compiles successfully but errors at request time because the header is absent,
		// so date receives an empty string.
		expression, err := manager.CompileAnyExpression("date(subgraph.response.header.Get('X-Missing'))")
		require.NoError(t, err)

		exprAttributes := []requestlogger.ExpressionAttribute{
			{
				Key:     "server_start",
				Default: "fallback",
				Expr:    expression,
			},
		}

		response := SubgraphAccessLogsFieldHandler(
			logger,
			nil,
			exprAttributes,
			nil,
			nil,
			nil,
			&expr.Context{},
		)

		require.Len(t, response, 1)
		require.Equal(t, "server_start", response[0].Key)
		require.Equal(t, "fallback", response[0].String)
	})

	t.Run("expression failing at runtime is skipped when no default is set", func(t *testing.T) {
		t.Parallel()

		logger := zap.NewNop()

		manager := expr.CreateNewExprManager()
		expression, err := manager.CompileAnyExpression("date(subgraph.response.header.Get('X-Missing'))")
		require.NoError(t, err)

		exprAttributes := []requestlogger.ExpressionAttribute{
			{
				Key:  "server_start",
				Expr: expression,
			},
		}

		response := SubgraphAccessLogsFieldHandler(
			logger,
			nil,
			exprAttributes,
			nil,
			nil,
			nil,
			&expr.Context{},
		)

		for _, f := range response {
			require.NotEqual(t, "server_start", f.Key)
		}
	})

	t.Run("logs operation subgraph fetch count when monday tweak is enabled", func(t *testing.T) {
		t.Parallel()

		require.True(t, mondaytweaks.ExposeOperationSubgraphFetchCountContextField)

		req, err := http.NewRequest(http.MethodPost, "http://localhost:3002/graphql", nil)
		require.NoError(t, err)

		rcc := buildRequestContext(requestContextOptions{r: req})
		rcc.operation = &operationContext{
			preparedPlan: &planWithMetaData{
				preparedPlan: &plan.SynchronousResponsePlan{
					Response: &resolve.GraphQLResponse{
						Fetches: resolve.Sequence(
							resolve.Single(&resolve.SingleFetch{Info: &resolve.FetchInfo{DataSourceName: "monolith"}}),
							resolve.Single(&resolve.SingleFetch{Info: &resolve.FetchInfo{DataSourceName: "users"}}),
							resolve.Single(&resolve.SingleFetch{Info: &resolve.FetchInfo{DataSourceName: "monolith"}}),
						),
					},
				},
			},
		}
		req = req.WithContext(withRequestContext(req.Context(), rcc))

		response := RouterAccessLogsFieldHandler(
			&zap.Logger{},
			[]config.CustomAttribute{{
				Key: "operation_subgraph_fetch_count",
				ValueFrom: &config.CustomDynamicAttribute{
					ContextField: ContextFieldOperationSubgraphFetchCount,
				},
			}},
			make([]requestlogger.ExpressionAttribute, 0),
			nil,
			req,
			nil,
			nil,
		)

		require.Len(t, response, 2)
		require.Equal(t, "operation_subgraph_fetch_count", response[1].Key)
		require.Equal(t, int64(3), response[1].Integer)
	})

}
