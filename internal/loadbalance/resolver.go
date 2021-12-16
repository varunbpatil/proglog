package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/varunbpatil/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

var _ resolver.Builder = (*Resolver)(nil)
var _ resolver.Resolver = (*Resolver)(nil)

type Resolver struct {
	mu sync.Mutex
	// clientConn connection is the user’s client connection and gRPC passes it to the
	// resolver for the resolver to update with the servers it discovers.
	clientConn resolver.ClientConn
	// resolverConn is the resolver’s own client connection to the server so it can call
	// GetServers() and get the servers.
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	// Build() receives the data needed to build a resolver that can discover the
	// servers (like the target address) and the client connection the resolver will
	// update with the servers it discovers. Build() sets up a client connection to our
	// server so the resolver can call the GetServers() API.
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...) //lint:ignore SA1019 Ignore deprecation warning
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "proglog"

func (r *Resolver) Scheme() string {
	// Scheme() returns the resolver’s scheme identifier. When you call grpc.Dial, gRPC
	// parses out the scheme from the target address you gave it and tries to find a
	// resolver that matches, defaulting to its DNS resolver. For our resolver, you’ll
	// format the target address like this: proglog://your-service- address.
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// get cluster and then set on clientConn attributes
	client := api.NewLogClient(r.resolverConn)
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New( // Tell the picker what server is the leader and what servers are the followers.
				"is_leader",
				server.IsLeader,
			),
		})
	}
	// Services can specify how clients should balance their calls to the service by
	// updating the state with a service config. We update the state with a service
	// config that specifies to use the “proglog” load balancer we’ll write later.
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
