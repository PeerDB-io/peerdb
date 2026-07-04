package gen

var Checklist = []string{
	"head.alter_table", "head.alter_online", "head.alter_ignore", "head.alter_online_ignore", "head.if_exists", "head.wait_nowait", "head.schema_qualified",
	"spec.add_column", "spec.add_column_if_not_exists", "spec.add_column_list", "spec.modify_column", "spec.change_column", "spec.drop_column", "spec.rename_column",
	"spec.add_index", "spec.add_fulltext_spatial", "spec.add_vector_index", "spec.add_primary", "spec.add_unique", "spec.add_foreign_key", "spec.add_check",
	"spec.add_period", "spec.system_versioning", "spec.alter_default", "spec.alter_visibility", "spec.alter_index_visibility", "spec.alter_check",
	"spec.drop_constraint", "spec.drop_index", "spec.keys_toggle", "spec.rename_table", "spec.rename_index", "spec.order_by", "spec.convert_charset",
	"spec.default_charset", "spec.tablespace", "spec.algorithm", "spec.lock", "spec.force", "spec.validation", "spec.secondary_load", "spec.table_options", "spec.partition_options",
	"type.integer", "type.fixed", "type.float", "type.bit_bool", "type.serial", "type.datetime", "type.char_text", "type.binary_blob", "type.enum_set", "type.json",
	"type.spatial", "type.vector", "type.mariadb_udt", "type.oracle",
	"attr.nullability", "attr.default", "attr.auto_key", "attr.comment", "attr.charset_collate", "attr.column_format_storage", "attr.engine_attribute", "attr.visibility",
	"attr.generated", "attr.check", "attr.references", "attr.serial_default", "attr.compressed", "attr.ref_system_id", "attr.system_versioning",
	"rename.table", "rename.tables", "rename.multi_pair", "rename.schema_qualified", "rename.wait_nowait",
	"benign.create", "benign.drop", "benign.insert", "benign.set_statement",
	"lex.identifiers", "lex.comments", "lex.strings", "lex.whitespace", "lex.nul_semicolon_chain",
}
