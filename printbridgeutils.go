package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/brotherlogic/goserver/utils"
	kmpb "github.com/brotherlogic/keymapper/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/brotherlogic/hometaskqueue/proto"
)

func (s *Server) getLastRun() (int64, error) {
	ctx, cancel := utils.ManualContext("ghc", "ghc", time.Minute, false)
	conn, err := s.FDialServer(ctx, "keymapper")
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown {
			log.Fatalf("Cannot reach keymapper: %v", err)
		}
		return -1, err
	}
	client := kmpb.NewKeymapperServiceClient(conn)
	resp, err := client.Get(ctx, &kmpb.GetRequest{Key: "hometaskqueue_last"})
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown || status.Convert(err).Code() == codes.InvalidArgument {
			log.Fatalf("Cannot read external: %v", err)
		}
		return -1, err
	}
	cancel()

	val, err := strconv.ParseInt(resp.Key.GetValue(), 10, 64)
	if err != nil {
		return -1, err
	}

	return val, nil
}

func (s *Server) runLoop() error {
	val, err := s.getLastRun()
	if err != nil {
		return err
	}

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
		Since:   val,
	})

	if err != nil {
		return err
	}

	if len(res.GetTasks()) == 0 {
		s.Log(fmt.Sprintf("Found %v tasks", len(res.GetTasks())))
	} else {
		s.Log(fmt.Sprintf("Found %v tasks -> %v", len(res.GetTasks()), res.GetTasks()[0].GetDateAdded()))
	}

	return nil
}
