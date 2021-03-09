package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/brotherlogic/hometaskqueue/proto"
)

func (s *Server) runLoop() error {
	ctx, cancel := utils.ManualContext("pbu", "pbu", time.Minute, false)
	defer cancel()

	systemRoots, err := x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("failed to load system root CA cert pool")
	}
	creds := credentials.NewTLS(&tls.Config{
		RootCAs: systemRoots,
	})
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial("hometaskqueue-q2ijxfqena-uw.a.run.app:443", opts...)
	if err != nil {
		return err
	}

	client := pb.NewHomeTaskQueueServiceClient(conn)
	if err != nil {
		return err
	}

	res, err := client.GetTasks(ctx, &pb.GetTasksRequest{
		QueueId: s.key,
	})

	if err != nil {
		return err
	}

	s.Log(fmt.Sprintf("Found %v tasks", len(res.GetTasks())))
	return nil
}
