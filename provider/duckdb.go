package provider

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/featureform/featureform/provider/offline"
	"github.com/duckdb/duckdb/driver"
)

type DuckDBProvider struct {
	db *sql.DB
}

func (p *DuckDBProvider) Connect(ctx context.Context) error {
	connStr := fmt.Sprintf("duckdb:///")
	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		return err
	}

	p.db = db
	return nil
}

func (p *DuckDBProvider) Disconnect(ctx context.Context) error {
	return p.db.Close()
}

func (p *DuckDBProvider) StoreFeature(ctx context.Context, feature *Feature) error {
	stmt, err := p.db.Prepare(`INSERT INTO features (name, value) VALUES (?, ?)`)
	if err != nil {
		return err
	}

	defer stmt.Close()

	_, err = stmt.Exec(feature.Name, feature.Value)
	if err != nil {
		return err
	}

	return nil
}

func (p *DuckDBProvider) GetFeature(ctx context.Context, name string) (*Feature, error) {
	stmt, err := p.db.Prepare(`SELECT value FROM features WHERE name = ?`)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	var value interface{}
	err = stmt.QueryRow(name).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &Feature{
		Name: name,
		Value: value,
	}, nil
}

func (p *DuckDBProvider) ListFeatures(ctx context.Context) ([]*Feature, error) {
	rows, err := p.db.Query(`SELECT name, value FROM features`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var features []*Feature
	for rows.Next() {
		var name string
		var value interface{}

		err := rows.Scan(&name, &value)
		if err != nil {
			return nil, err
		}

		features = append(features, &Feature{
			Name: name,
			Value: value,
		})
	}

	return features, nil
}

func (p *DuckDBProvider) DeleteFeature(ctx context.Context, name string) error {
	stmt, err := p.db.Prepare(`DELETE FROM features WHERE name = ?`)
	if err != nil {
		return err
	}

	defer stmt.Close()

	_, err = stmt.Exec(name)
	if err != nil {
		return err
	}

	return nil
}

func (p *DuckDBProvider) TransformFeature(ctx context.Context, feature *Feature, transformation *Transformation) (*Feature, error) {
	// TODO: Implement DuckDB transformation engine

	return nil, fmt.Errorf("DuckDB transformation engine not yet implemented")
}
