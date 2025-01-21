package main

import (
	"context"
	"entropy/internal/model"
	"entropy/internal/nanoid"
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

	// run embedded server and create client
	nc, ns, err := RunEmbeddedServer(false, true)
	if err != nil {
		log.Error("Error running embedded server", "err", err)
		return
	}
	if nc != nil {
		defer nc.Drain()
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("Error creating jetstream", "err", err)
		return
	}

	ctx := context.Background()

	stream, err := CreateWQStream(ctx, js, "cleanup_handler")
	if err != nil {
		log.Error("Error creating cleanup_handler stream", "err", err)
		return
	}

	// create a cleanup handler consumer
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		Name:          "cleanup-1",
		FilterSubject: "cleanup_handler.>",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	// consume messages from the cleanup_handler stream
	go func() {
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

			pipelinestatusname := fmt.Sprintf("kv-%s", pplid)
			err = js.DeleteKeyValue(ctx, pipelinestatusname)
			if err != nil {
				log.Warn("Error deleting pipeline status", "err", err)
			}

			_ = m.Ack()
		})
		if err != nil {
			log.Error("Error consuming messages", "err", err)
			return
		}
	}()

	for i := 0; i < 1000; i++ {
		go func() {
			id := GetId()
			bucket := fmt.Sprintf("kv-%s", id)
			kv, err := createKV(ctx, js, bucket)
			if err != nil {
				log.Error("Error creating and deleting kv stream", "err", err)
				return
			}

			// creating a dangling watcher on purpose
			// could be initiated on another process / machine
			watcher, err := kv.WatchAll(ctx)
			if err != nil {
				log.Error("Error watching kv", "err", err)
				return
			}

			go func() {
				for {
					entry := <-watcher.Updates()
					log.Debugf("Received entry %#v", entry)
					if entry == nil {
						watcher.Stop()
						break
					}
				}
			}()

			_, err = kv.Create(ctx, "hello.world", []byte("world"))
			if err != nil {
				log.Error("Error creating kv value", "err", err)
			}
			time.Sleep(100 * time.Millisecond)
			_, err = kv.Update(ctx, "hello.world", []byte("Pierre"), 1)
			time.Sleep(1000 * time.Millisecond)

			subject := fmt.Sprintf("cleanup_handler.%s", id)
			_, err = js.Publish(ctx, subject, []byte(""))
			if err != nil {
				log.Error("Error publishing message", "err", err)
			}
		}()
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

func createKV(ctx context.Context, js jetstream.JetStream, name string) (jetstream.KeyValue, error) {

	cfg := jetstream.KeyValueConfig{
		Bucket: name,
		TTL:    60 * time.Minute,
	}
	kv, err := js.CreateKeyValue(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

func RunEmbeddedServer(inProcess bool, enableLogging bool) (*nats.Conn, *server.Server, error) {
	natsfs := filepath.Join(config.NATS_SERVER_ROOTFS, "nats-data")
	natsfs = strings.ReplaceAll(natsfs, "\\", "/")
	// if there's a folder with a name that starts with a r, replace it with /r - Windows path confusion
	natsfs = strings.ReplaceAll(natsfs, "\r", "/r")

	// create folder if it doesn't exist
	_ = os.MkdirAll(natsfs, os.ModePerm)

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

	clientOpts := []nats.Option{}

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

func GetId() string {
	id, err := nanoid.Generate("0123456789abcdefghijklmnopqrstuvwxyz", 21)

	if err != nil {
		return ""
	}
	return id

}
