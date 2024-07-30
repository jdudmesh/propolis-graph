package hub

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"

	"github.com/jdudmesh/propolis/internal/model"
	"github.com/quic-go/quic-go"
)

const defaultHubPort = 9000

type internalStateStore interface {
	CreateConnection(cn model.ClientConnection) error
}

type worker struct {
	hostAddr    string
	store       internalStateStore
	quit        chan struct{}
	connections []*clientConnection
}

func New(configHost string, configPort int, store internalStateStore) (*worker, error) {
	if configHost == "" {
		configHost = "localhost"
	}
	if configPort == 0 {
		configPort = defaultHubPort
	}

	return &worker{
		hostAddr: fmt.Sprintf("%s:%d", configHost, configPort),
		store:    store,
		quit:     make(chan struct{}),
	}, nil
}

func (w *worker) Run() error {
	listener, err := quic.ListenAddr(w.hostAddr, generateTLSConfig(), nil)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Println("Waiting for connections on: " + w.hostAddr)

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			return err
		}

		s, err := conn.AcceptStream(ctx)
		if err != nil {
			return err
		}

		client, err := NewClientConn(s)
		if err != nil {
			return err
		}

		err = w.store.CreateConnection(client.ClientConnection)
		if err != nil {
			return err
		}
		w.connections = append(w.connections, client)

		go client.Run()
	}
}

func (w *worker) Close() error {
	close(w.quit)
	return nil
}

func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"propolis"},
	}
}
