package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/jdudmesh/propolis/internal/bloom"
	"github.com/jdudmesh/propolis/internal/graph"
	"github.com/jdudmesh/propolis/internal/identity"
	"github.com/jdudmesh/propolis/internal/model"
	"github.com/jdudmesh/propolis/internal/node"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var httpClient *http.Client
var nodeID string
var filter *bloom.Filter

type Peer interface {
	Run() error
	CountOfPeers() (int, error)
	PublishIdentity(id *identity.Identity) error
	Execute(id *identity.Identity, action string) error
}

func main() {
	nodeID = model.NewID()

	db, err := sqlx.Connect("mysql", "root:CKYwALUCTIOnEsiNGTRoTiO@tcp(127.0.0.1:3306)/notthetalk")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		}, // set a TLS client config, if desired
		QUICConfig: &quic.Config{}, // QUIC connection options
	}
	defer roundTripper.Close()

	httpClient = &http.Client{
		Transport: roundTripper,
	}

	cwd := os.Getenv("WORKSPACE_DIR")
	if cwd == "" {
		cwd, err = os.Getwd()
		if err != nil {
			panic(err)
		}
	}

	databaseUrl := fmt.Sprintf("file:%s/data/identity.db?mode=rwc&_secure_delete=true", cwd)
	fmt.Println(databaseUrl)
	store, err := identity.NewStore(databaseUrl)
	if err != nil {
		panic(err)
	}

	svc, err := identity.NewService(store)
	if err != nil {
		panic(err)
	}

	id, err := svc.GetPrimaryIdentity()
	if err != nil {
		panic(err)
	}

	peer, err := createPeer()
	if err != nil {
		panic(err)
	}

	filter.Set([]byte(id.Identifier))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = peer.Run()
		if err != nil {
			panic(err)
		}
	}()

	// for {
	// 	n, err := peer.CountOfPeers()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("Number of peers: %d\n", n)
	// 	if n > 0 {
	// 		break
	// 	}
	// 	time.Sleep(1 * time.Second)
	// }

	err = peer.PublishIdentity(id)
	if err != nil {
		panic(err)
	}

	wg.Wait()
	// err = sendFolders(id, db)
	// if err != nil {
	// 	slog.Error("folders", "error", err)
	// }
}

func createPeer() (Peer, error) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))

	config := node.Config{
		Config: graph.Config{
			Logger:           logger,
			GraphDatabaseURL: "file::graph9003.db?mode=memory&cache=shared",
		},
		Type:            node.NodeTypePeer,
		Host:            "127.0.0.1",
		Port:            9003,
		NodeDatabaseURL: "file::node9003.db?mode=memory&cache=shared",
		Seeds:           []string{"127.0.0.1:9000"},
	}

	filter = bloom.New()
	filter.Set([]byte("hello"))

	peer, err := node.New(config, filter)
	if err != nil {
		return nil, err
	}

	return peer, nil
}

func PublishIdentity(peer Peer, id *identity.Identity) error {
	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: id.CertificateData}))
	certPEMEncoded, err := json.Marshal(certPEM)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(certPEMEncoded))

	sb := strings.Builder{}
	sb.WriteString("MERGE (:Identity{")
	props := []string{
		fmt.Sprintf("id:'%s'", id.Identifier),
		fmt.Sprintf("handle:'%s'", id.Handle),
		fmt.Sprintf("bio:'%s'", id.Bio),
		fmt.Sprintf("certificate:'%s'", string(certPEMEncoded)),
	}
	sb.WriteString(strings.Join(props, ", "))
	sb.WriteString("})")

	err = peer.Execute(id, sb.String())
	if err != nil {
		return err
	}

	return nil
}

func sendFolders(id *identity.Identity, db *sqlx.DB) error {
	rows, err := db.Queryx("select id, description from folder limit 1")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		f := &Folder{}
		err = rows.StructScan(f)
		if err != nil {
			return err
		}

		action := fmt.Sprintf("MERGE (:Tag{extId:'%d', value:'%s'})", f.ID, f.Description)
		err = postAction(id, action)
		if err != nil {
			return err
		}
	}

	return nil
}

func postAction(id *identity.Identity, action string) error {
	ctx := context.Background()

	stm := strings.NewReader(action)
	req, err := http.NewRequestWithContext(ctx, "POST", "https://localhost:9000/action", stm)
	if err != nil {
		return err
	}

	var privateKey ed25519.PrivateKey
	for _, key := range id.Keys {
		if key.Type == identity.KeyTypeED25519PrivateKey {
			privateKey = key.Data
			break
		}
	}
	if privateKey == nil {
		log.Fatalf("No private key found")
	}

	actionID := model.NewID()
	h := sha256.New()
	h.Write([]byte(id.Identifier))
	h.Write([]byte(actionID))
	h.Write([]byte(action))
	sig := ed25519.Sign(privateKey, h.Sum(nil))

	encodedSig := base64.StdEncoding.EncodeToString(sig)
	log.Printf("Signature: %s", encodedSig)

	req.Header.Add(node.HeaderIdentifier, id.Identifier)
	req.Header.Add(node.HeaderActionID, actionID)
	req.Header.Add(node.HeaderNodeID, nodeID)
	req.Header.Add(node.HeaderSignature, encodedSig)

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}

	slog.Info("response", "status", resp.StatusCode)
	return nil
}

type Folder struct {
	ID          int    `db:"id"`
	Description string `db:"description"`
}
