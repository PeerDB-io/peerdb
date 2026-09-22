package peerflow

import (
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// setQRepSourceTableConfig carries mapping's source specific config over to cfg, the QRepConfig of
// the mapping's initial load.
func setQRepSourceTableConfig(cfg *protos.QRepConfig, mapping *protos.TableMapping) error {
	switch config := mapping.SourceTableConfig.(type) {
	case nil:
	case *protos.TableMapping_MongoTableConfig:
		cfg.SourceTableConfig = &protos.QRepConfig_MongoTableConfig{MongoTableConfig: config.MongoTableConfig}
	default:
		return fmt.Errorf("unsupported source table config %T for table %s", config, mapping.SourceTableIdentifier)
	}
	return nil
}

// setTableMappingSourceTableConfig carries cfg's source specific config over to mapping, the table
// mapping a QRep flow hands to the setup activities.
func setTableMappingSourceTableConfig(mapping *protos.TableMapping, cfg *protos.QRepConfig) error {
	switch config := cfg.SourceTableConfig.(type) {
	case nil:
	case *protos.QRepConfig_MongoTableConfig:
		mapping.SourceTableConfig = &protos.TableMapping_MongoTableConfig{MongoTableConfig: config.MongoTableConfig}
	default:
		return fmt.Errorf("unsupported source table config %T for table %s", config, mapping.SourceTableIdentifier)
	}
	return nil
}
