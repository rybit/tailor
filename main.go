package main

import (
	"log"
	"strings"

	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"time"

	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats"
	"github.com/spf13/cobra"
)

var tlsKey string
var tlsCert string
var tlsCACert string
var debugEnabled bool
var quietOutput bool
var tlsEnabled bool
var useChannel bool

func main() {
	rootCmd := cobra.Command{
		Short: "natail",
		Long:  "natail subject",
		Run:   run,
	}

	rootCmd.Flags().BoolVar(&tlsEnabled, "tls", false, "Enable TLS, do not verify clients")
	rootCmd.Flags().StringVar(&tlsCert, "tlscert", "", "Server certificate file")
	rootCmd.Flags().StringVar(&tlsKey, "tlskey", "", "Private key for server certificate")
	rootCmd.Flags().StringVar(&tlsCACert, "tlscacert", "", "Client certificate CA for verification")
	rootCmd.Flags().BoolVarP(&debugEnabled, "debug", "d", false, "enable debug logging")
	rootCmd.Flags().BoolVarP(&quietOutput, "quiet", "q", false, "silence the actual printing of messages")
	rootCmd.Flags().BoolVarP(&useChannel, "channel", "c", false, "use a channel subscription")

	rootCmd.Flags().StringSliceP("servers", "s", []string{"localhost:4222"}, "Which servers to use")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}
}

func run(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		log.Fatal("Must provide a subject")
	}
	subject := args[0]

	servers, err := cmd.Flags().GetStringSlice("servers")
	if err != nil {
		log.Fatal("Failed to get list of servers: " + err.Error())
	}

	serverString := strings.Join(servers, ",")
	var nc *nats.Conn
	if tlsEnabled {
		tlsConfig, err := getTLSConfig()
		if err != nil {
			log.Fatal("Failed to load certificate: " + err.Error())
		}

		nc, err = nats.Connect(serverString, nats.Secure(tlsConfig), nats.ErrorHandler(handleError))
	} else {
		nc, err = nats.Connect(serverString)
	}

	var msgsRx int64

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Printf("\nReceived %d messages\n", msgsRx)
		os.Exit(0)
	}()

	debug("subscribing to subject: " + subject)
	if useChannel {
		ch := make(chan *nats.Msg, 100000)
		sub, err := nc.ChanSubscribe(subject, ch)
		if err != nil {
			log.Fatal("Failed to subscribe to " + subject + " because of " + err.Error())
		}
		defer sub.Unsubscribe()

		for msg := range ch {
			msgsRx += 1
			handleMsg(msg, msgsRx)
		}
	} else {
		sub, err := nc.SubscribeSync(subject)
		if err != nil {
			log.Fatal("Failed to subscribe to " + subject + " because of " + err.Error())
		}
		defer sub.Unsubscribe()
		if err := sub.SetPendingLimits(-1, -1); err != nil {
			log.Fatal("Failed to unlimit subscription")
		}
		for {
			msg, err := sub.NextMsg(time.Hour)
			if err != nil {
				log.Fatal("Problem waiting for message: " + err.Error())
			}
			msgsRx += 1
			handleMsg(msg, msgsRx)
		}
	}
}

func handleError(conn *nats.Conn, sub *nats.Subscription, err error) {
	fmt.Println("error: " + err.Error())
}

func handleMsg(msg *nats.Msg, msgsRx int64) {
	if quietOutput {
		fmt.Printf("Messages Received: %d\r", msgsRx)
		if msgsRx%100000 == 0 || msgsRx == 1 {
			debug(fmt.Sprintf("Messages Received: %d", msgsRx))
		}
	} else {
		fmt.Println(string(msg.Data))
	}
}

func debug(msg string) {
	if debugEnabled {
		log.Printf("DEBUG: %s\n", msg)
	}
}

func getTLSConfig() (*tls.Config, error) {
	debug("Setting up TLS connection")
	debug("cert file: " + tlsCert)
	debug("ca file:" + tlsCACert)
	debug("key file:" + tlsKey)
	pool := x509.NewCertPool()
	caData, err := ioutil.ReadFile(notEmpty(tlsCACert))
	if err != nil {
		return nil, err
	}

	if !pool.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("Failed to add CA cert")
	}
	cert, err := tls.LoadX509KeyPair(notEmpty(tlsCert), notEmpty(tlsKey))
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	return tlsConfig, nil
}

func notEmpty(val string) string {
	if val == "" {
		log.Fatal("Can't provide an empty string")
	}

	return val
}
