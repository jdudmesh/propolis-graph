package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jdudmesh/propolis/internal/node"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var httpClient *http.Client
var nodeID string

func main() {
	nodeID = gonanoid.Must()

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

	err = sendFolders(db)
	if err != nil {
		slog.Error("folders", "error", err)
	}
}

func sendFolders(db *sqlx.DB) error {
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
		err = postAction(action)
		if err != nil {
			return err
		}
	}

	return nil
}

func postAction(action string) error {
	ctx := context.Background()

	stm := strings.NewReader(action)
	req, err := http.NewRequestWithContext(ctx, "POST", "https://localhost:9000/action", stm)
	if err != nil {
		return err
	}
	req.Header.Add(node.HeaderActionID, "")
	req.Header.Add(node.HeaderNodeID, nodeID)

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
