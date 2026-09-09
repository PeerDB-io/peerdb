-- +goose Up
ALTER TABLE flows
ADD COLUMN config_proto BYTEA;
