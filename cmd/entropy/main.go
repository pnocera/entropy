package main

import (
	"context"
	"entropy/internal/model"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var config *model.Config

func main() {

	config = initConfig()

	nc, ns, err := RunEmbeddedServer(true, true)
	if err != nil {
		fmt.Println(err)
		return
	}
	if nc != nil {
		defer nc.Drain()
	}

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx := context.Background()

	stream, err := CreateWQStream(ctx, js, "cleanup_handler")
	if err != nil {
		fmt.Println(err)
		return
	}
	_, _ = CreateWQStream(ctx, js, "pplrun_nats-server_pipeline")

	_, _ = CreateWQStream(ctx, js, "tskrun")

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		Name:          "cleanup-1",
		FilterSubject: "cleanup_handler.>",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = cons.Consume(func(m jetstream.Msg) {
		if m == nil {
			return
		}
		subject := m.Subject()
		// extract the pipeline id from the subject ( the pipeline id is the string after . in the subject )
		pplid := strings.Split(subject, ".")[1]

		if pplid == "" {
			_ = m.Ack()
			return
		}

		log.Info("Cleaning up pipeline", "pipeline", pplid)

		pipelinestatusname := fmt.Sprintf("pplstatus_%s", pplid)
		err = js.DeleteKeyValue(ctx, pipelinestatusname)
		if err != nil {
			log.Warn("Error deleting pipeline status", "err", err)
		}

		pipelinetasksname := fmt.Sprintf("ppltasks_%s", pplid)
		err = js.DeleteKeyValue(ctx, pipelinetasksname)
		if err != nil {
			log.Warn("Error deleting pipeline tasks", "err", err)
		}

		objstorename := fmt.Sprintf("%s", pplid)
		err = js.DeleteObjectStore(ctx, objstorename)
		if err != nil {
			log.Warn("Error deleting object store", "err", err)
		}

		_ = m.Ack()
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	ns.WaitForShutdown()
}

func CreateWQStream(ctx context.Context, js jetstream.JetStream, name string) (jetstream.Stream, error) {
	cfg := jetstream.StreamConfig{
		Name:      name,
		Retention: jetstream.WorkQueuePolicy,
		Subjects:  []string{name + ".>"},
	}
	return js.CreateOrUpdateStream(ctx, cfg)

}

func RunEmbeddedServer(inProcess bool, enableLogging bool) (*nats.Conn, *server.Server, error) {
	natsfs := filepath.Join(config.NATS_SERVER_ROOTFS, "nats-data")
	natsfs = strings.ReplaceAll(natsfs, "\\", "/")
	natsfs = strings.ReplaceAll(natsfs, "\r", "/r")

	opts := &server.Options{
		ServerName:         "entropy",
		JetStream:          true,
		JetStreamDomain:    "embedded",
		JetStreamMaxMemory: 2 * 1024 * 1024 * 1024,
		JetStreamMaxStore:  100 * 1024 * 1024 * 1024,
		StoreDir:           natsfs,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}

	if enableLogging {
		ns.ConfigureLogger()
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, fmt.Errorf("unable to start NATS Server")
	}

	clientOpts := []nats.Option{
		//nats.MaxReconnects(10),
		//nats.ReconnectWait(5 * time.Second),
		//nats.Timeout(1 * time.Second),
	}

	if inProcess {
		clientOpts = append(clientOpts, nats.InProcessServer(ns))
	}

	nc, err := nats.Connect(ns.ClientURL(), clientOpts...)
	if err != nil {
		return nil, nil, err
	}

	return nc, ns, err

}

func initConfig() *model.Config {
	exepath, _ := os.Executable()

	rootfolder := filepath.Dir(exepath)
	err := os.Chdir(rootfolder)
	if err != nil {
		log.Fatal(err)
	}

	viper.AddConfigPath(rootfolder)
	viper.SetConfigType("env")
	viper.SetConfigFile(".env")
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", "filename", viper.ConfigFileUsed())
	} else {
		log.Error("Error reading config file:", "err", err)
	}

	conf := &model.Config{}
	err = viper.Unmarshal(conf)
	if err != nil {
		log.Error("Error unmarshalling config", "err", err)
		return nil
	}
	return conf

}
