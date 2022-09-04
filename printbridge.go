package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/brotherlogic/goserver/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbg "github.com/brotherlogic/goserver/proto"
	kmpb "github.com/brotherlogic/keymapper/proto"
)

//Server main server type
type Server struct {
	*goserver.GoServer
	key string
}

// Init builds the server
func Init() *Server {
	s := &Server{
		GoServer: &goserver.GoServer{},
	}
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {

}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*pbg.State {
	return []*pbg.State{
		&pbg.State{Key: "magic", Value: int64(12345)},
	}
}

func main() {
	var quiet = flag.Bool("quiet", false, "Show all output")
	flag.Parse()

	//Turn off logging
	if *quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	server := Init()
	server.PrepServer("printbridge")
	server.Register = server

	ctx, cancel := utils.ManualContext("ghc", time.Minute)
	conn, err := server.FDialServer(ctx, "keymapper")
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown {
			log.Fatalf("Cannot reach keymapper: %v", err)
		}
		return
	}
	client := kmpb.NewKeymapperServiceClient(conn)
	resp, err := client.Get(ctx, &kmpb.GetRequest{Key: "hometaskqueue_id"})
	if err != nil {
		if status.Convert(err).Code() == codes.Unknown || status.Convert(err).Code() == codes.InvalidArgument {
			log.Fatalf("Cannot read external: %v", err)
		}
		return
	}
	server.key = resp.GetKey().GetValue()
	cancel()

	err = server.RegisterServerV2(false)
	if err != nil {
		return
	}

	go func() {
		for true {
			ctx, cancel := utils.ManualContext("pb-loop", time.Minute)
			err = server.runLoop()
			if err != nil {
				server.CtxLog(ctx, fmt.Sprintf("Unable to run loop: %v", err))
			}
			cancel()
			time.Sleep(time.Hour)
		}
	}()

	fmt.Printf("%v", server.Serve())
}
