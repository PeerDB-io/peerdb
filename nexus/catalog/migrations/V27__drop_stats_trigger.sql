CREATE OR REPLACE FUNCTION clear_mirror_stats() RETURNS TRIGGER AS $$
DECLARE
    mirrorName TEXT;
    cloneMirrorRegex TEXT;
BEGIN
    mirrorName := NEW.name;
    cloneMirrorRegex := 'clone_' || mirrorName || '_%';

    -- Initial Load Stats
    DELETE FROM peerdb_stats.qrep_partitions WHERE flow_name ILIKE cloneMirrorRegex;
    DELETE FROM peerdb_stats.qrep_runs WHERE flow_name ILIKE cloneMirrorRegex;

    -- CDC Stats
    DELETE FROM peerdb_stats.cdc_batches WHERE flow_name = mirrorName;
    DELETE FROM peerdb_stats.cdc_batch_table WHERE flow_name = mirrorName;
    DELETE FROM peerdb_stats.cdc_flows WHERE flow_name = mirrorName;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on inserts to flow table
CREATE TRIGGER before_insert_clear_stats
BEFORE INSERT ON flows
FOR EACH ROW
EXECUTE FUNCTION clear_mirror_stats();
