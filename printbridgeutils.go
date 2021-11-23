package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/brotherlogic/goserver/utils"
	kmpb "github.com/brotherlogic/keymapper/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	ghpb "github.com/brotherlogic/githubcard/proto"
	pb "github.com/brotherlogic/hometaskqueue/proto"
	ppb "github.com/brotherlogic/printer/proto"
)

func (s *Server) getLastRun() (int64, error) {
	ctx, cancel := utils.ManualContext("ghc", time.Minute)
	defer cancel()
	conn, err := s.FDialServer(ctx, "keymapper")
	defer conn.Close()
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

	val, err := strconv.ParseInt(resp.Key.GetValue(), 10, 64)
	if err != nil {
		return -1, err
	}

	return val, nil
}

func (s *Server) setLastRun(val int64) error {
	ctx, cancel := utils.ManualContext("ghc", time.Minute)
	defer cancel()
	conn, err := s.FDialServer(ctx, "keymapper")
	defer conn.Close()
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown {
			log.Fatalf("Cannot reach keymapper: %v", err)
		}
		return err
	}
	client := kmpb.NewKeymapperServiceClient(conn)
	_, err = client.Set(ctx, &kmpb.SetRequest{Key: "hometaskqueue_last", Value: fmt.Sprintf("%v", val)})
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown || status.Convert(err).Code() == codes.InvalidArgument {
			log.Fatalf("Cannot read external: %v", err)
		}
		return err
	}

	return nil
}

func (s *Server) print(ctx context.Context, task *pb.Task) error {
	// Skip all STO tasks
	if strings.HasPrefix(task.GetBody(), "STO") {
		return nil
	}

	conn, err := s.FDialServer(ctx, "printer")
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown {
			log.Fatalf("Cannot reach printer: %v", err)
		}
		return err
	}
	defer conn.Close()

	client := ppb.NewPrintServiceClient(conn)
	_, err = client.Print(ctx, &ppb.PrintRequest{
		Lines: strings.Split(task.GetBody(), "\n"),
	})
	return err
}

func (s *Server) github(task *pb.Task) error {
	ctx, cancel := utils.ManualContext("pb-github", time.Minute)
	defer cancel()
	conn, err := s.FDialServer(ctx, "githubcard")
	defer conn.Close()
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown {
			log.Fatalf("Cannot reach printer: %v", err)
		}
		return err
	}
	client := ghpb.NewGithubClient(conn)
	_, err = client.AddIssue(ctx, &ghpb.Issue{
		Title:   task.GetTitle(),
		Body:    task.GetBody(),
		Service: task.GetComponent(),
	})
	return err
}

func (s *Server) runLoop() error {
	ctx, cancel := utils.ManualContext("pbu", time.Minute)
	defer cancel()

	key, err := s.RunLockingElection(ctx, "printbridgelock")
	if err != nil {
		return err
	}
	defer s.ReleaseLockingElection(ctx, "printbridgelock", key)

	val, err := s.getLastRun()
	if err != nil {
		return err
	}

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
	defer conn.Close()
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
		s.Log(fmt.Sprintf("Found %v tasks (%v)", len(res.GetTasks()), val))
	} else {

		oldest := res.GetTasks()[0].GetDateAdded()
		for _, task := range res.GetTasks() {
			if task.GetDateAdded() > oldest {
				oldest = task.GetDateAdded()
			}

			var err error
			if task.GetType() == pb.TaskType_PRINTER {
				err = s.print(ctx, task)
			} else if task.GetType() == pb.TaskType_GITHUB {
				err = s.github(task)
			}

			if err != nil {
				return err
			}
		}

		err = s.setLastRun(oldest)
		s.Log(fmt.Sprintf("Found %v tasks -> %v (%v) now %v (%v)", len(res.GetTasks()), res.GetTasks()[0].GetDateAdded(), val, oldest, err))
		if err != nil {
			return err
		}
	}

	return nil
}
